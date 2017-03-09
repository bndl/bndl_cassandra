# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import Sequence
from functools import lru_cache, partial
from threading import Lock
import contextlib
import queue
import random

from cassandra import OperationTimedOut, ReadTimeout, WriteTimeout, CoordinationFailure, Unavailable
from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy, HostDistance, TokenAwarePolicy

from bndl.util.pool import ObjectPool


TRANSIENT_ERRORS = (Unavailable, ReadTimeout, WriteTimeout, OperationTimedOut, CoordinationFailure)


class LocalNodeFirstPolicy(TokenAwarePolicy):
    def __init__(self, local_hosts):
        super().__init__(DCAwareRoundRobinPolicy())
        self._local_hosts = local_hosts
        self._hosts = {}


    def is_node_local(self, host):
        return host.address in self._local_hosts


    def populate(self, cluster, hosts):
        self._hosts = {h.address:h for h in hosts if h.is_up}
        super().populate(cluster, hosts)


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
            used = set()

            # if replica addresses are set on the prepared_statement, use these first
            prepared_statement = getattr(query, 'prepared_statement', None)
            replicas = getattr(prepared_statement, 'replicas', None)
            if replicas:
                # shuffle to avoid hitting on the same node over and over again
                replicas = list(replicas)
                random.shuffle(replicas)
                # yield the replicas (if known and up)
                for replica in replicas:
                    try:
                        host = self._hosts.get(replica)
                        if host and host.is_up:
                            used.add(host)
                            yield host
                    except KeyError:
                        pass

            # use the dc aware round robin policy if there is no routing key
            # other wise use the routing key to resolve the right replicas
            routing_key = query.routing_key
            if routing_key is None or keyspace is None:
                for host in child.make_query_plan(keyspace, query):
                    yield host
            else:
                replicas = [replica for replica in
                            self._cluster_metadata.get_replicas(keyspace, routing_key)
                            if replica.is_up]

                # prefer a replica which is local (share IP address)
                for replica in replicas:
                    if self.is_node_local(replica):
                        yield replica

                # dc-local replica's on other machines
                for replica in replicas:
                    if child.distance(replica) == HostDistance.LOCAL and not self.is_node_local(replica):
                        yield replica

                used.update(replicas)

                # fall back to dc-aware round robin for other hosts (e.g. on network partition)
                for host in child.make_query_plan(keyspace, query):
                    # skip if we've already listed this host
                    if host not in used or child.distance(host) == HostDistance.REMOTE:
                        yield host

#
    def on_up(self, host):
        self._hosts[host.address] = host
        return super().on_up(host)

    def on_down(self, host):
        self._hosts.pop(host.address, None)
        return super().on_down(host)

    def on_add(self, host):
        self._hosts[host.address] = host
        return super().on_add(host)

    def on_remove(self, host):
        self._hosts.pop(host.address, None)
        return super().on_remove(host)


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
            contact_points |= worker.ip_addresses()
    if not contact_points:
        contact_points = ctx.node.ip_addresses()
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
                load_balancing_policy=LocalNodeFirstPolicy(ctx.node.ip_addresses()),
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
