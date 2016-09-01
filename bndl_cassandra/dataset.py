from functools import partial
import difflib
import functools
import logging

from bndl.compute.dataframes import DataFrame, DistributedDataFrame, \
    combine_dataframes
from bndl.compute.dataset import Dataset, Partition
from bndl.util import funcs
from bndl.util.retry import do_with_retry
from bndl_cassandra import partitioner
from bndl_cassandra.coscan import CassandraCoScanDataset
from bndl_cassandra.session import cassandra_session, TRANSIENT_ERRORS
from cassandra import protocol
from cassandra.query import tuple_factory, named_tuple_factory, dict_factory


logger = logging.getLogger(__name__)


def _did_you_mean(msg, word, possibilities):
    matches = difflib.get_close_matches(word, possibilities, n=2)
    if matches:
        msg += ', did you mean ' + ' or '.join(matches) + '?'
    return msg


def get_table_meta(session, keyspace, table):
    try:
        keyspace_meta = session.cluster.metadata.keyspaces[keyspace]
    except KeyError as e:
        msg = 'Keyspace %s not found' % (keyspace,)
        msg = _did_you_mean(msg, keyspace, session.cluster.metadata.keyspaces.keys())
        raise KeyError(msg) from e
    try:
        return keyspace_meta.tables[table]
    except KeyError as e:
        msg = 'Table %s.%s not found' % (keyspace, table)
        msg = _did_you_mean(msg, table, keyspace_meta.tables.keys())
        raise KeyError(msg) from e


class CassandraScanDataset(Dataset):
    def __init__(self, ctx, keyspace, table, contact_points=None):
        '''
        Create a scan across keyspace.table.

        :param ctx:
            The compute context.
        :param keyspace: str
            Keyspace of the table to scan.
        :param table: str
            Name of the table to scan.
        :param contact_points: None or str or [str,str,str,...]
            None to use the default contact points or a list of contact points
            or a comma separated string of contact points.
        '''
        super().__init__(ctx)
        self.keyspace = keyspace
        self.table = table
        self.contact_points = contact_points
        self._row_factory = named_tuple_factory
        self._protocol_handler = None

        self._select = None
        self._limit = None
        self._where = '''
            token({partition_key_column_names}) > ? and
            token({partition_key_column_names}) <= ?
        '''.format(
            partition_key_column_names=', '.join(c.name for c in self.meta.partition_key)
        )


    @property
    @functools.lru_cache(1)
    def meta(self):
        with self.ctx.cassandra_session(contact_points=self.contact_points) as session:
            return get_table_meta(session, self.keyspace, self.table)


    def count(self, push_down=None):
        if push_down is True or (not self.cached and push_down is None):
            return self.select('count(*)').as_tuples().map(funcs.getter(0)).sum()
        else:
            return super().count()


    def as_tuples(self):
        return self._with(_row_factory=tuple_factory)

    def as_dicts(self):
        return self._with(_row_factory=dict_factory)

    def as_dataframe(self):
        import pandas as pd

        primary_key = [c.name for c in self.meta.primary_key if not self._select or c.name in self._select]

        # creates dicts with column names and numpy arrays per query page
        arrays = self._with(_row_factory=tuple_factory, _protocol_handler='NumpyProtocolHandler')

        # converts each dict of numpy arrays (a query page) to a pandas dataframe
        def to_df(arrays):
            if len(primary_key) == 0:
                index = None
            elif len(primary_key) == 1:
                name = primary_key[0]
                index = pd.Index(arrays.pop(name), name=name)
            else:
                index = [arrays.pop(name) for name in primary_key]
                index = pd.MultiIndex.from_arrays(index, names=primary_key)
            return DataFrame(arrays, index)

        # creates one df per partition
        def as_df(part):
            return combine_dataframes(to_df(arrays) for arrays in part)

        # take first to get meta data
        sample = next(arrays.limit(1).map_partitions(as_df).icollect(eager=False, parts=True))

        # create the DDF
        return DistributedDataFrame.from_sample(arrays.map_partitions(as_df), sample)


    def select(self, *columns):
        return self._with(_select=columns)


    def limit(self, num):
        return self._with(_limit=int(num))

    def itake(self, num):
        if not self.cached and not self._limit:
            return self.limit(num).itake(num)
        else:
            return super().itake(num)


    def coscan(self, other, keys=None):
        assert isinstance(other, CassandraScanDataset)
        return CassandraCoScanDataset(self, other, keys=keys)


    @functools.lru_cache(1)
    def parts(self):
        with cassandra_session(self.ctx, contact_points=self.contact_points) as session:
            partitions = partitioner.partition_ranges(self.ctx, session, self.keyspace, self.table)

        return [
            CassandraScanPartition(self, i, *part)
            for i, part in enumerate(partitions)
        ]


    def query(self, session):
        select = ', '.join(self._select) if self._select else '*'
        limit = ' limit %s' % self._limit if self._limit else ''
        query = '''
            select {select}
            from {keyspace}.{table}
            where {where}{limit}
        '''.format(
            select=select,
            keyspace=self.keyspace,
            table=self.table,
            where=self._where,
            limit=limit
        )
        return session.prepare(query)



class CassandraScanPartition(Partition):
    def __init__(self, dset, part_idx, replicas, token_ranges, size_estimate_mb, size_estimate_keys):
        super().__init__(dset, part_idx)
        self.replicas = set(replicas)
        self.token_ranges = token_ranges
        self.size_estimate_mb = size_estimate_mb
        self.size_estimate_keys = size_estimate_keys


    def _fetch_token_range(self, session, token_range):
        query = self.dset.query(session)
        query.consistency_level = self.dset.ctx.conf.get('bndl_cassandra.read_consistency_level')

        if logger.isEnabledFor(logging.INFO):
            logger.info('executing query %s for token_range %s', query.query_string.replace('\n', ''), token_range)

        timeout = self.dset.ctx.conf.get('bndl_cassandra.read_timeout')
        resultset = session.execute(query, token_range, timeout=timeout)

        results = []
        while True:
            has_more = resultset.response_future.has_more_pages
            if has_more:
                resultset.response_future.start_fetching_next_page()
            results.extend(resultset.current_rows)
            if has_more:
                resultset = resultset.response_future.result()
            else:
                break

        return results


    def _materialize(self, ctx):
        retry_count = max(0, ctx.conf.get('bndl_cassandra.read_retry_count'))
        retry_backoff = ctx.conf.get('bndl_cassandra.read_retry_backoff')

        with ctx.cassandra_session(contact_points=self.dset.contact_points) as session:
            old_row_factory = session.row_factory
            old_protocol_handler = session.client_protocol_handler
            try:
                session.row_factory = self.dset._row_factory
                if self.dset._protocol_handler:
                    session.client_protocol_handler = getattr(protocol, self.dset._protocol_handler)
                logger.debug('scanning %s token ranges with query %s', len(self.token_ranges))
                for token_range in self.token_ranges:
                    yield from do_with_retry(partial(self._fetch_token_range, session, token_range),
                                             retry_count, retry_backoff, TRANSIENT_ERRORS)
            finally:
                session.row_factory = old_row_factory
                session.client_protocol_handler = old_protocol_handler


    def _preferred_workers(self, workers):
        return [
            worker
            for worker in workers
            if worker.ip_addresses & self.replicas
        ]
