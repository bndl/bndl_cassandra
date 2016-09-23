from bndl_cassandra.tests import CassandraTest

key_count = 10
row_count = 100
rows = [dict(key=str(i % key_count), cluster=i, varint_val=i) for i in range(row_count)]
tuple_rows = [((row['key'], row['cluster']), row['varint_val']) for row in rows]

class JoinTest(CassandraTest):
    def setUp(self):
        super().setUp()
        self.ctx.collection(rows).cassandra_save(self.keyspace, self.table).execute()
        self.join_tuples = (self.ctx.collection(tuple_rows)
                                .join_with_cassandra(self.keyspace, self.table))
        self.join_dicts = (self.ctx.collection(rows)
                                .join_with_cassandra(self.keyspace, self.table)
                                .on(key=['key', 'cluster'])
                                .as_dicts())


    def test_join_on_primary_kv_tuples(self):
        join = self.join_tuples
        self.assertEqual(join.count(), row_count)
        for ((key, cluster), varint_val), right in join.collect():
            self.assertEqual(key, right.key)
            self.assertEqual(cluster, right.cluster)
            self.assertEqual(varint_val, right.varint_val)


    def test_join_on_primary_dicts(self):
        join = self.join_dicts
        self.assertEqual(join.count(), row_count)
        for left, right in join.collect():
            self.assertEqual(left['varint_val'], right['varint_val'])


    def test_join_on_partition(self):
        left = self.ctx.collection(rows)
        join = self.join_dicts.on(['key'], ['key'])
        self.assertEqual(join.count(), row_count)
        for left, right in join.collect():
            self.assertEqual(len(right), key_count)
            for row in right:
                self.assertEqual(left['key'], row['key'])


    def test_join_select(self):
        join = self.join_dicts.select('varint_val').collect()
        for left, right in join:
            self.assertEqual(list(right.keys()), ['varint_val'])
            self.assertEqual(left['varint_val'], right['varint_val'])


    def test_multijoin(self):
        join = (self.join_dicts
                    .select('varint_val')
                    .starmap(lambda left, right: ((left['key'], left['cluster']), right))
                    .join_with_cassandra(self.keyspace, self.table)
                    .as_dicts()
                    .select('varint_val'))

        for ((key, cluster), second), third in join.collect():
            self.assertEqual(int(cluster), second['varint_val'])
            self.assertEqual(second['varint_val'], third['varint_val'])
