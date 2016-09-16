from bndl.util.funcs import getter, key_or_getter
from bndl_cassandra.dataset import _CassandraDataset
from bndl.compute.dataset import  Partition


QUERY_TEMPLATE = '''select {select} from {keyspace}.{table} where {where}'''

JOIN_TYPES = ['inner', 'left']
INNER, LEFT = JOIN_TYPES


class CassandraJoinDataset(_CassandraDataset):
    def __init__(self, src, keyspace, table, contact_points=None):
        super().__init__(src.ctx, keyspace, table, contact_points=contact_points)
        self.src = src
        self._on = [c.name for c in self.meta.primary_key]
        self._on_primary = True
        self._key = getter(0)
        self._join_type = INNER

    @property
    def _where(self):
        return ' and '.join('%s = ?' % c for c in self._on)

    def on(self, columns=None, key=None):
        '''
        Join on particular columns (from the primary key in the Cassandra
        table) and set a key to select the corresponding values from the
        dataset to join.

        :param columns: sequence[string] optional
            The columns to join on. Must be a left subset of the primary key.
            The following must hold: primary_key[:len(columns)] == columns.
            When the full primary key is selected, the rows yielded will be
            single elements (a dict or namedtuple representing the selected
            row). When only part of the primary key is selected (the
            partition key columns and perhaps some clustering columns), a list
            of selected rows is yielded.

        :param key: callable(element), sequence[string], object optional
            The key for getting the values to query Cassandra with. Must be a
            * callable which returns a sequence for each element in this
              dataset with the values to use.
            * or a sequence of strings to be used as index in each element
              in a operator.itemgetter(*columns) fashion (i.e. using the
              __getitem__ protocol)
            * or a plain value to be used with the __getitem__ mechanism
        '''
        if not columns and not key:
            return self
        opts = {}
        if columns is not None:
            primary_key = [c.name for c in self.meta.primary_key]
            for idx, (pk, c) in enumerate(zip(primary_key, columns)):
                if pk != c:
                    raise ValueError('Column %s at %r not in primary key %r or'
                                     ' in wrong position' % (c, idx, primary_key))
            opts['_on'] = columns
            opts['_on_primary'] = columns == primary_key
        if key is not None:
            opts['_key'] = key_or_getter(key)
        return self._with(**opts)

    def inner(self):
        '''
        Yield only rows which have a corresponding row in the Cassandra table.
        '''
        return self._with(_join_type=INNER)

    def left(self):
        '''
        Yield rows regardless of whether a row was selected from Cassandra.
        '''
        return self._with(_join_type=LEFT)

    def parts(self):
        return [
            CassandraJoinPartition(self, src)
            for src in self.src.parts()
        ]


class CassandraJoinPartition(Partition):
    def __init__(self, dset, src):
        super().__init__(dset, src.idx, src)

    def _materialize(self, ctx):
        key = self.dset._key
        join_type = self.dset._join_type
        on_primary = self.dset._on_primary
        data = self.src.materialize(ctx)

        with self.dset._session() as session:
            query = session.prepare(self.dset.query)
            for element in data:
                params = key(element)
                rows = list(session.execute(query, params))
                if join_type == INNER and not rows:
                    continue
                if on_primary:
                    if rows:
                        rows = rows[0]
                    else:
                        rows = None
                yield element, rows