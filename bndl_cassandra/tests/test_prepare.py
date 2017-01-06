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

from bndl_cassandra.session import _prepare
from bndl_cassandra.tests import CassandraTest


class QueryPrepareTest(CassandraTest):
    def test_prepare_caching(self):
        hits_count_start = self.ctx.range(32, pcount=16).map_partitions(lambda p: [_prepare.cache_info()[3]]).sum()

        # trigger query preparation
        dset = self.ctx.cassandra_table(self.keyspace, self.table)
        for i in range(self.ctx.worker_count):
            targeted = dset.require_workers(lambda workers: [list(workers)[i]])
            self.assertEqual(targeted.collect(), [])

        # check that prepared query is cached
        hits_count = self.ctx.range(32, pcount=16).map_partitions(lambda p: [_prepare.cache_info()[3]]).sum()
        self.assertEqual(hits_count - hits_count_start , 16)  # @UndefinedVariable
