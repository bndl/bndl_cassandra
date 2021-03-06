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

from enum import Enum

from bndl.compute.context import ComputeContext
from bndl.compute.dataset import Dataset
from bndl.util.conf import Int, Float, CSV, String, Bool, Attr
from bndl.util.funcs import as_method
from bndl_cassandra.dataset import CassandraScanDataset
from bndl_cassandra.save import cassandra_save, cassandra_execute
from bndl_cassandra.session import cassandra_session
from bndl_cassandra.join import CassandraJoinDataset
from cassandra import ConsistencyLevel


class BatchKey(Enum):
    none = 0
    replica_set = 1
    partition_key = 2



# Configuration
contact_points = CSV()
port = Int(9042)
keyspace = String()
compression = Bool(True)
metrics_enabled = Bool(True)

read_retry_count = Int(3)
read_retry_backoff = Float(2, desc='delay = {read_timeout_backoff} ^ retry_round - 1')
read_timeout = Int(120)
read_consistency_level = Attr(ConsistencyLevel.LOCAL_ONE, obj=ConsistencyLevel)
fetch_size_rows = Int(1000)
part_size_keys = Int(100 * 1000)
part_size_mb = Int(64)

write_retry_count = Int(3)
write_retry_backoff = Float(2, desc='delay = {write_timeout_backoff} ^ retry_round - 1')
write_timeout = Int(120)
write_concurrency = Int(2)
write_consistency_level = Attr(ConsistencyLevel.LOCAL_QUORUM, obj=ConsistencyLevel)
write_batch_key = Attr(BatchKey.none, obj=BatchKey)
write_batch_size = Int(50, desc='maximum size of a batch in kilobytes')
write_batch_buffer_size = Int(1000)


# Bndl API extensions
ComputeContext.cassandra_session = as_method(cassandra_session)
ComputeContext.cassandra_table = as_method(CassandraScanDataset)
Dataset.cassandra_execute = as_method(cassandra_execute)
Dataset.cassandra_save = as_method(cassandra_save)
Dataset.join_with_cassandra = as_method(CassandraJoinDataset)
