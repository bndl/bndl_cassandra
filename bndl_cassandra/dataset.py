from functools import partial
import difflib
import functools
import logging

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
        '''
        Create a bndl.compute.dataframe.DistributedDataFrame out of a Cassandra
        table scan.

        When primary key fields are selected, they are used to compose a
        (multilevel) index.

        Example::

            >>> df = ctx.cassandra_table('ks', 'tbl').as_dataframe()
            >>> df.collect()
                                                 comments
            id          timestamp
            ZIJr6BDGCeo 2014-10-09 19:28:43.657         1
                        2015-01-12 20:24:49.947         4
                        2015-01-13 02:24:30.931         39
            kxcT9VOI0oU 2015-01-12 14:24:16.378         1
                        2015-01-12 20:24:49.947         5
                        2015-01-13 02:24:30.931         8
                        2015-02-04 10:29:58.118         4
            A_egyclRPOw 2015-12-16 13:50:53.210         1
                        2015-01-18 18:28:19.556         2
                        2015-01-22 22:28:33.358         4
                        2015-01-27 02:28:59.578         6
                        2015-01-31 06:29:07.937         7
        '''
        import pandas as pd
        from bndl.compute.dataframes import DataFrame, DistributedDataFrame, \
                                            combine_dataframes

        if self._select:
            pk_cols_selected = [c.name for c in self.meta.primary_key if c.name in self._select]
        else:
            pk_cols_selected = [c.name for c in self.meta.primary_key]

        # creates dicts with column names and numpy arrays per query page
        arrays = self._with(_row_factory=tuple_factory, _protocol_handler='NumpyProtocolHandler')

        # converts each dict of numpy arrays (a query page) to a pandas dataframe
        def to_df(arrays):
            if len(pk_cols_selected) == 0:
                index = None
            elif len(pk_cols_selected) == 1:
                name = pk_cols_selected[0]
                index = pd.Index(arrays.pop(name), name=name)
            else:
                index = [arrays.pop(name) for name in pk_cols_selected]
                index = pd.MultiIndex.from_arrays(index, names=pk_cols_selected)
            return DataFrame(arrays, index)

        # creates one df per partition
        def as_df(part):
            return combine_dataframes(to_df(arrays) for arrays in part)

        # take first to get meta data
        sample = next(arrays.limit(1).map_partitions(as_df).icollect(eager=False, parts=True))

        # create the DDF
        return DistributedDataFrame.from_sample(arrays.map_partitions(as_df), sample)


    def span_by(self, *cols):
        '''
        Span by groups rows in a Cassandra table scan by a subset of the
        primary key.

        This is useful for tables with clustering columns: rows in a
        cassandra table scan are returned clustered by partition key
        and sorted by clustering columns. This is exploited to efficiently
        (without shuffle) group rows by a part of the primary key.

        Example::

            >>> tbl = ctx.cassandra_table('ks', 'tbl')
            >>> tbl.span_by().collect()
            [('ZIJr6BDGCeo',                       comments
              id          timestamp
              ZIJr6BDGCeo 2014-10-09 19:28:43.657         1
                          2015-01-12 20:24:49.947         4
                          2015-01-13 02:24:30.931         9),
             ('kxcT9VOI0oU',                       comments
              id          timestamp
              kxcT9VOI0oU 2015-01-12 14:24:16.378         1
                          2015-01-12 20:24:49.947         2
                          2015-01-13 02:24:30.931         5
                          2015-02-04 10:29:58.118         8),
             ('A_egyclRPOw',                       comments
              id          timestamp
              A_egyclRPOw 2015-12-16 13:50:53.210         1
                          2015-01-18 18:28:19.556         2
                          2015-01-22 22:28:33.358         4
                          2015-01-27 02:28:59.578         6
                          2015-01-31 06:29:07.937         7)]

        A Cassandra table scan spanned by part of the primary key consists of
        pandas.DataFrame objects, and thus allows for easy per group analysis.

            >>> for key, count in tbl.span_by().map_values(lambda e: e.count()).collect():
            ...     print(key, ':', count)
            ...
            ZIJr6BDGCeo : comments    3
            dtype: int64
            kxcT9VOI0oU : comments    4
            dtype: int64
            A_egyclRPOw : comments    5
            dtype: int64
        '''

        pk_cols = [c.name for c in self.meta.primary_key]

        if not cols:
            cols = pk_cols[:-1]
        else:
            if not all(col in pk_cols for col in cols):
                raise ValueError('Can only span a cassandra table scan by '
                                 'columns from the primary key')
        if self._select:
            if len(self._select) < len(cols):
                raise ValueError('Span by on a Cassandra table requires at '
                                 'least selection of the partition key')
            elif not all(a == b for a, b in zip(self._select, cols)):
                raise ValueError('The columns to span by should have the same '
                                 'order as those selected')
        if not all(a == b for a, b in zip(pk_cols, cols)) or \
           len(cols) >= len(pk_cols):
            raise ValueError('The columns to span by should be a subset '
                             'of and have the same order as primary key')

        levels = list(range(len(cols)))
        return self.as_dataframe().map_partitions(lambda part: part.groupby(level=levels))


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
