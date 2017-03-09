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

from functools import partial
from itertools import chain
import difflib
import functools
import logging
import time

from cassandra import protocol
from cassandra.protocol import ErrorMessage
from cassandra.query import tuple_factory, named_tuple_factory, dict_factory

from bndl.compute.dataset import Dataset, Partition, NODE_LOCAL
from bndl.util import funcs
from bndl.util.callsite import callsite
from bndl.util.funcs import prefetch
from bndl.util.retry import do_with_retry, retry_delay
from bndl_cassandra import partitioner
from bndl_cassandra.coscan import CassandraCoScanDataset
from bndl_cassandra.session import TRANSIENT_ERRORS


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



class _CassandraDataset(Dataset):
    def __init__(self, ctx, keyspace, table, contact_points=None):
        super().__init__(ctx)
        self.keyspace = keyspace
        self.table = table
        self.contact_points = contact_points
        self._row_factory = named_tuple_factory
        self._protocol_handler = None
        self._select = None
        self._limit = None

    def _session(self):
        client_protocol_handler = (getattr(protocol, self._protocol_handler)
                                   if self._protocol_handler else None)
        return self.ctx.cassandra_session(contact_points=self.contact_points,
                                          row_factory=self._row_factory,
                                          client_protocol_handler=client_protocol_handler)

    @property
    @functools.lru_cache(1)
    def meta(self):
        with self._session() as session:
            return do_with_retry(partial(get_table_meta, session, self.keyspace, self.table),
                                 1, 2, TRANSIENT_ERRORS)

    def as_tuples(self):
        return self._with(_row_factory=tuple_factory)

    def as_dicts(self):
        return self._with(_row_factory=dict_factory)

    def select(self, *columns):
        return self._with(_select=columns)

    def limit(self, num):
        return self._with(_limit=int(num))

    @property
    def query(self):
        select = ', '.join(self._select) if self._select else '*'
        limit = ' limit %s' % self._limit if self._limit else ''
        query = ('select {select} '
                 'from {keyspace}.{table} '
                 'where {where}{limit} '
                 'allow filtering')
        query = query.format(
            select=select,
            keyspace=self.keyspace,
            table=self.table,
            where=self._where,
            limit=limit
        )
        return query


# converts each dict of numpy arrays (a query page) to a pandas dataframe
def _arrays_to_df(pk_cols_selected, arrays):
    import pandas as pd
    from bndl.compute.dataframes import DataFrame

    if len(pk_cols_selected) == 0:
        index = None
    elif len(pk_cols_selected) == 1:
        name = pk_cols_selected[0]
        index = pd.Index(arrays.pop(name), name=name)
    else:
        index = [arrays.pop(name) for name in pk_cols_selected]
        index = pd.MultiIndex.from_arrays(index, names=pk_cols_selected)

    return DataFrame(arrays, index)



class CassandraScanDataset(_CassandraDataset):
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
        super().__init__(ctx, keyspace, table, contact_points)
        self._where_tokens = ('token({partition_key_column_names}) > ? and '
                              'token({partition_key_column_names}) <= ?').format(
                                partition_key_column_names=', '.join(c.name for c in self.meta.partition_key)
                            )
        self._where_clause = ''
        self._where_values = ()


    @property
    def _where(self):
        return ' and '.join(filter(None, ((self._where_tokens, self._where_clause))))


    def where(self, clause, *values):
        return self._with(_where_clause=clause, _where_values=values)


    @callsite()
    def count(self, push_down=None):
        if push_down is True or (not self.cached and push_down is None):
            return self.select('count(*)').as_tuples().map(funcs.getter(0)).sum()
        else:
            return super().count()


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
        from bndl.compute.dataframes import DistributedDataFrame

        if self._select:
            pk_cols_selected = [c.name for c in self.meta.primary_key if c.name in self._select]
        else:
            pk_cols_selected = [c.name for c in self.meta.primary_key]
        to_df = partial(_arrays_to_df, pk_cols_selected)

        # determine index names
        index = pk_cols_selected or [None]
        # determine column names
        if self._select:
            columns = self._select
        else:
            columns = self.meta.columns
        columns = [c for c in columns if c not in pk_cols_selected]

        # creates dicts with column names and numpy arrays per query page
        arrays = self._with(_row_factory=tuple_factory, _protocol_handler='NumpyProtocolHandler')
        # convert to dataframes
        dfs = arrays.map(to_df).map_partitions(pd.concat)
        return DistributedDataFrame(dfs, index, columns)


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
        from bndl.compute.dataframes import DataFrame

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
        return self.as_dataframe().map_partitions(partial(DataFrame.groupby, level=levels))


    def itake(self, num):
        if not self.cached and not self._limit:
            return self.limit(num).itake(num)
        else:
            return super().itake(num)


    def coscan(*scans, keys=None):
        return CassandraCoScanDataset(*scans, keys=keys)


    def parts(self):
        partitions = partitioner.partition_ranges(self.ctx, self.contact_points, self.keyspace, self.table)
        return [
            CassandraScanPartition(self, i, *part)
            for i, part in enumerate(partitions)
        ]



class CassandraScanPartition(Partition):
    def __init__(self, dset, part_idx, replicas, token_ranges, size_estimate_mb, size_estimate_keys):
        super().__init__(dset, part_idx)
        self.replicas = set(replicas)
        self.token_ranges = token_ranges
        self.size_estimate_mb = size_estimate_mb
        self.size_estimate_keys = size_estimate_keys


    def _compute(self):
        retry_count = max(0, self.dset.ctx.conf.get('bndl_cassandra.read_retry_count'))
        retry_backoff = self.dset.ctx.conf.get('bndl_cassandra.read_retry_backoff')

        timeout = self.dset.ctx.conf.get('bndl_cassandra.read_timeout')
        consistency_level = self.dset.ctx.conf.get('bndl_cassandra.read_consistency_level')

        with self.dset._session() as session:
            logger.debug('scanning %s token ranges', len(self.token_ranges))

            try:
                query = session.prepare(self.dset.query)
            except ErrorMessage as exc:
                raise Exception('Unable to prepare query %s' % self.dset.query) from exc

            query.consistency_level = consistency_level
            query.replicas = self.replicas

            for token_range in self.token_ranges:
                if logger.isEnabledFor(logging.INFO):
                    logger.info('executing query %s for token_range %s',
                                query.query_string.replace('\n', ''), token_range)

                params = token_range + self.dset._where_values
                execute_async = partial(session.execute_async, query, params, timeout=timeout)

                # perform initial query without paging_state
                paging_state = None
                future = execute_async()

                # no. (transient) failures must be <= retry_count
                fails = 0

                while True:
                    try:
                        # wait for previous page
                        result_set = future.result()
                        paging_state = result_set.paging_state
                    except TRANSIENT_ERRORS:
                        # handle transient errors (check max no. retries, possibly back-off with
                        # sleep and execute query for current page again)
                        fails += 1
                        if not retry_count or fails > retry_count:
                            raise
                        elif retry_backoff:
                            sleep = retry_delay(retry_backoff, fails)
                            time.sleep(sleep)

                        future = execute_async(paging_state=paging_state)
                    else:
                        if paging_state:
                            # start fetch of next page (if any)
                            future = execute_async(paging_state=paging_state)
                        # hand of current page
                        yield from result_set.current_rows
                        # stop if no more pages left
                        if not paging_state:
                            break


    def _locality(self, workers):
        return (
            (worker, NODE_LOCAL)
            for worker in workers
            if worker.ip_addresses() & self.replicas
        )
