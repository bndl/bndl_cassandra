from collections import Sequence
from functools import lru_cache, partial
from threading import Lock
import contextlib
import queue

from bndl.util.pool import ObjectPool
from cassandra import OperationTimedOut, ReadTimeout, WriteTimeout, CoordinationFailure, Unavailable
from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy, HostDistance, TokenAwarePolicy


TRANSIENT_ERRORS = (Unavailable, ReadTimeout, WriteTimeout, OperationTimedOut, CoordinationFailure)


class LocalNodeFirstPolicy(TokenAwarePolicy):
    def __init__(self, child_policy, local_hosts):
        super().__init__(child_policy)
        self._local_hosts = local_hosts


    def is_node_local(self, host):
        return host.address in self._local_hosts


    def make_query_plan(self, working_keyspace=None, query=None):
        if query and query.keyspace:
            keyspace = query.keyspace
        else:
            keyspace = working_keyspace

        child = self._child_policy
        if query is None:
            for host in child.make_query_plan(keyspace, query):
                yield host
        else:
            routing_key = query.routing_key
            if routing_key is None or keyspace is None:
                for host in child.make_query_plan(keyspace, query):
                    yield host
            else:
                replicas = [replica for replica in
                            self._cluster_metadata.get_replicas(keyspace, routing_key)
                            if replica.is_up]
                
                for replica in replicas:
                    if self.is_node_local(replica):
                        yield replica

                for replica in replicas:
                    # local replica's on other machines
                    if child.distance(replica) == HostDistance.LOCAL and not self.is_node_local(replica):
                        yield replica

                for host in child.make_query_plan(keyspace, query):
                    # skip if we've already listed this host
                    if host not in replicas or child.distance(host) == HostDistance.REMOTE:
                        yield host


_PREPARE_LOCK = Lock()


@lru_cache()
def _prepare(self, query, custom_payload=None):
    return Session.prepare(self, query, custom_payload)


def prepare(self, query, custom_payload=None):
    with _PREPARE_LOCK:
        return _prepare(self, query, custom_payload)


def get_contact_points(ctx, contact_points):
    if not contact_points:
        contact_points = ctx.conf.get('bndl_cassandra.contact_points')
    if isinstance(contact_points, str):
        contact_points = (contact_points,)
    return _get_contact_points(ctx, *(contact_points or ()))


@lru_cache()
def _get_contact_points(ctx, *contact_points):
    if not contact_points:
        contact_points = set()
        for worker in ctx.workers:
            contact_points |= worker.ip_addresses
    if not contact_points:
        contact_points = ctx.node.ip_addresses
    if isinstance(contact_points, str):
        contact_points = [contact_points]
    if isinstance(contact_points, Sequence) and len(contact_points) == 1 and isinstance(contact_points[0], str):
        contact_points = contact_points[0].split(',')
    return tuple(sorted(contact_points))


@contextlib.contextmanager
def cassandra_session(ctx, keyspace=None, contact_points=None,
                      row_factory=None, client_protocol_handler=None):
    # get hold of the dict of pools (keyed by contact_points)
    pools = getattr(cassandra_session, 'pools', None)
    if not pools:
        cassandra_session.pools = pools = {}
    if not keyspace:
        keyspace = ctx.conf.get('bndl_cassandra.keyspace')

    # determine contact points, either given or IP addresses of the workers
    contact_points = get_contact_points(ctx, contact_points)
    # check if there is a cached session object
    pool = pools.get((contact_points, keyspace))

    # or create one if not
    if not pool:
        def create_cluster():
            return Cluster(
                contact_points,
                port=ctx.conf.get('bndl_cassandra.port'),
                compression=ctx.conf.get('bndl_cassandra.compression'),
                load_balancing_policy=LocalNodeFirstPolicy(DCAwareRoundRobinPolicy(), ctx.node.ip_addresses),
                metrics_enabled=ctx.conf.get('bndl_cassandra.metrics_enabled'),
            )

        def create():
            '''create a new session'''
            pool = pools[(contact_points, keyspace)]
            cluster = getattr(pool, 'cluster', None)
            if not cluster or cluster.is_shutdown:
                pool.cluster = cluster = create_cluster()
            session = cluster.connect(keyspace)
            session.prepare = partial(prepare, session)
            session.default_fetch_size = ctx.conf.get('bndl_cassandra.fetch_size_rows')

            return session

        def check(session):
            '''check if the session is not closed'''
            return not session.is_shutdown

        pools[(contact_points, keyspace)] = pool = ObjectPool(create, check, max_size=4)

    # take a session from the pool, yield it to the caller
    # and put the session back in the pool
    session = pool.get()

    old_row_factory = session.row_factory
    old_protocol_handler = session.client_protocol_handler

    try:
        if row_factory:
            session.row_factory = row_factory
        if client_protocol_handler:
            session.client_protocol_handler = client_protocol_handler

        yield session

    finally:
        session.row_factory = old_row_factory
        session.client_protocol_handler = old_protocol_handler

        try:
            pool.put(session)
        except queue.Full:
            session.shutdown()
