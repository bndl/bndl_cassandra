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

from bndl_cassandra.tests import CassandraTest


class SaveTest(CassandraTest):
    num_rows = 1000

    def setUp(self):
        super().setUp()
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=True), 0)


    def test_save_dicts(self):
        dset = self.ctx.range(self.num_rows).map(lambda i: dict(key=str(i), cluster=i, varint_val=i))
        saved = (dset.cassandra_save(self.keyspace, self.table).sum())

        self.assertEqual(saved, self.num_rows)
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=True), self.num_rows)

        rows = self.ctx.cassandra_table(self.keyspace, self.table) \
                   .select('key', 'cluster', 'varint_val').as_dicts().collect()
        self.assertEqual(len(rows), len(dset.collect()))
        self.assertEqual(sorted(rows, key=lambda row: int(row['key'])), dset.collect())


    def test_save_tuples(self):
        dset = self.ctx.range(self.num_rows).map(lambda i: (str(i), i, i))
        saved = (dset.cassandra_save(
            self.keyspace, self.table,
            columns=('key', 'cluster', 'varint_val'),
            keyed_rows=False
        ).sum())

        self.assertEqual(saved, self.num_rows)
        self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=True), self.num_rows)

        rows = self.ctx.cassandra_table(self.keyspace, self.table) \
                       .select('key', 'cluster', 'varint_val').as_tuples().collect()
        self.assertEqual(len(rows), len(dset.collect()))
        self.assertEqual(sorted(rows, key=lambda row: int(row[0])), dset.collect())


    def test_save_batches(self):
        dset = self.ctx.range(self.num_rows).map(lambda i: (str(i // 100), i, i))
        for batch_key in ('none', 'replica_set', 'partition_key'):
            self.truncate()
            self.ctx.conf['bndl_cassandra.write_batch_key'] = batch_key
            self.ctx.conf['bndl_cassandra.write_batch_size'] = 20
            self.ctx.conf['bndl_cassandra.write_batch_buffer_size'] = 20

            saved = (dset.cassandra_save(
                self.keyspace, self.table,
                columns=('key', 'cluster', 'varint_val'),
                keyed_rows=False
            ).sum())

            self.assertEqual(saved, self.num_rows)
            self.assertEqual(self.ctx.cassandra_table(self.keyspace, self.table).count(push_down=True), self.num_rows)

            rows = self.ctx.cassandra_table(self.keyspace, self.table) \
                           .select('key', 'cluster', 'varint_val').as_tuples().collect()
            self.assertEqual(len(rows), len(dset.collect()))
            key = lambda row: (int(row[0]), row[1])
            rows.sort(key=key)
            self.assertEqual(rows, dset.sort(key).collect())
