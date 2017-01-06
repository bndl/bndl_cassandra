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

from bndl.compute.tests import DatasetTest


class CassandraTest(DatasetTest):
    keyspace = 'bndl_cassandra_test'
    table = 'test_table'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.ctx.conf['bndl_cassandra.contact_points'] = '127.0.0.1'

        try:
            with cls.ctx.cassandra_session() as session:
                session.execute('''
                    create keyspace if not exists {keyspace}
                    with replication = {{
                        'class': 'SimpleStrategy',
                        'replication_factor': '1'
                    }};
                '''.format(keyspace=cls.keyspace))

                session.execute('''
                    create table if not exists {keyspace}.{table} (
                        key text,
                        cluster int,
                        int_list list<int>,
                        double_set set<double>,
                        text_map map<text,text>,
                        timestamp_val timestamp,
                        varint_val varint,
                        primary key (key, cluster)
                    );
                '''.format(keyspace=cls.keyspace, table=cls.table))
        except:
            cls.ctx.stop()
            raise


    def setUp(self):
        super().setUp()
        self.truncate()

    def truncate(self, keyspace=None, table=None):
        truncate = 'truncate %s.%s;' % (keyspace or self.keyspace,
                                        table or self.table)
        with self.ctx.cassandra_session() as session:
            session.execute(truncate)
