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

from unittest.case import TestCase
import itertools
import random

from bndl_cassandra.partitioner import SizeEstimate, partition_ranges_, \
    T_COUNT, T_MIN, T_MAX


class PartitioningTest(TestCase):
    def test_partitioning(self):
        token_range_counts = (1, 16, 256, 256 * 4, 256 * 16)
        sizes = (100, 1000)  # in MB
        key_counts = (8251937, 18409199)
        fractions = (0.01, 0.1, 1)
        max_keys = (100 * 1000, 1000 * 1000,)

        random_points = [random.randint(T_MIN + 1, T_MAX - 1)
                         for _ in range(max(token_range_counts))]

        combinations = itertools.product(token_range_counts, sizes, key_counts, fractions, max_keys)
        for token_range_count, size, key_count, fraction, max_keys in combinations:
            # generate random ranges
            points = [T_MIN] + random_points[:token_range_count - 1] + [T_MAX]
            points.sort()
            ranges = list(zip(points, points[1:]))

            total_length = 0
            size_estimate = SizeEstimate(size, key_count, fraction)
            max_length = max_keys / size_estimate.token_size_keys
            partitioned = partition_ranges_(tuple(ranges), max_length, size_estimate)
            for ranges, size in partitioned:
                for start, end in ranges:
                    length = end - start
                    total_length += length
                    self.assertLess(start, end)
                    self.assertLess(length, max_length)
            self.assertEqual(total_length, T_COUNT - 1)
