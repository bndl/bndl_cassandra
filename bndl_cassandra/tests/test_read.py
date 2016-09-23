from bndl_cassandra.tests import CassandraTest


key_count = 10
row_count = 100
rows = [dict(key=str(i % key_count), cluster=str(i), varint_val=i) for i in range(row_count)]


class ReadTest(CassandraTest):
    def setUp(self):
        super().setUp()
        self.ctx.collection(rows).cassandra_save(self.keyspace, self.table).execute()

    def test_count(self):
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=False), row_count)
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=True), row_count)

    def test_cache(self):
        dset = self.ctx.cassandra_table(self.keyspace, self.table)
        self.assertEqual(dset.count(), row_count)  # count before
        self.assertEqual(dset.cache().count(), row_count)  # count while caching
        self.assertEqual(dset.count(), row_count)  # count after

    def test_collect_dicts(self):
        dicts = self.ctx.cassandra_table(self.keyspace, self.table).as_dicts()
        self.assertEqual(len(dicts.collect()), row_count)
        self.assertEqual(type(dicts.first()), dict)

    def test_collect_tuples(self):
        tuples = self.ctx.cassandra_table(self.keyspace, self.table).as_tuples()
        self.assertEqual(len(tuples.collect()), row_count)
        self.assertEqual(type(tuples.first()), tuple)

    def test_collect_dataframe(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest('Pandas not installed')
        df = self.ctx.cassandra_table(self.keyspace, self.table).as_dataframe()
        self.assertIn('key', df.index)
        self.assertIn('cluster', df.index)
        self.assertIn('varint_val', df.columns)
        self.assertEqual(len(df.collect()), row_count)
        self.assertIsInstance(df.first(), tuple)
        self.assertIsInstance(df.take(3), pd.DataFrame)

    def test_span_by(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest('Pandas not installed')
        df = self.ctx.cassandra_table(self.keyspace, self.table).span_by()
        self.assertEqual(df.count(), key_count)
        first = df.first()
        self.assertIsInstance(first, tuple)
        self.assertEqual(len(first), 2)
        self.assertIsInstance(first[1], pd.DataFrame)
        self.assertEqual(len(first[1]), row_count // key_count)
        take3 = df.take(3)
        self.assertIsInstance(take3, list)
        self.assertEqual(len(take3), 3)
        collected = df.collect()
        self.assertIsInstance(collected, list)
        self.assertEqual(len(collected), key_count)

    def test_select(self):
        columns = sorted(('key', 'cluster', 'varint_val'))
        rows = self.ctx.cassandra_table(self.keyspace, self.table).as_dicts().select(*columns).collect()
        self.assertEqual(len(rows), row_count)
        for row in rows:
            self.assertEqual(sorted(row.keys()), columns)

    def test_where(self):
        min_cluster = '50'
        rows = self.ctx.cassandra_table(self.keyspace, self.table).where('cluster > ?', min_cluster)
        for cluster in rows.select('cluster').pluck(0).icollect():
            self.assertGreater(cluster, min_cluster)

    def test_slicing(self):
        first = self.ctx.cassandra_table(self.keyspace, self.table).first()
        self.assertIn({k: v for k, v in first._asdict().items() if k in ('key', 'cluster', 'varint_val')}, rows)
        self.assertEqual(len(self.ctx.cassandra_table(self.keyspace, self.table).take(3)), 3)

    def test_missing(self):
        with self.assertRaisesRegex(KeyError, r'Keyspace {test.keyspace}x not found, did you mean {test.keyspace}?' .format(test=self)):
            self.ctx.cassandra_table(self.keyspace + 'x', self.table).first()
        with self.assertRaisesRegex(KeyError, r'Table {test.keyspace}.{test.table}x not found, did you mean {test.table}?' .format(test=self)):
            self.ctx.cassandra_table(self.keyspace, self.table + 'x').first()
