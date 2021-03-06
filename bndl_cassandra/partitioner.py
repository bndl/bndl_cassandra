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

from functools import lru_cache
from operator import itemgetter
import copy
import math
import random

from bndl_cassandra.session import cassandra_session
from cytoolz import itertoolz


T_COUNT = 2 ** 64
T_MIN = -(2 ** 63)
T_MAX = (2 ** 63) - 1


def get_token_ranges(ring):
    ring = [t.value for t in ring]

    if ring[0] != T_MIN:
        ring = [T_MIN] + ring
    if ring[-1] != T_MAX:
        ring = ring + [T_MAX]

    return list(zip(ring[:-1], ring[1:]))


def replicas_for_token(keyspace, token_map, token):
    replicas = token_map.get_replicas(keyspace, token_map.token_class(token))
    return tuple(replica.address for replica in replicas)


def ranges_by_replicas(session, keyspace):
    # get raw ranges from token ring
    token_map = session.cluster.metadata.token_map
    raw_ranges = get_token_ranges(token_map.ring)

    # group by replica
    return itertoolz.groupby(0, (
        (replicas_for_token(keyspace, token_map, start), start, end)
        for start, end in raw_ranges
    ))


class Bin(object):
    def __init__(self):
        self.ranges = []
        self.size = 0


def partition_ranges(ctx, contact_points, keyspace, table=None, size_estimates=None):
    max_size_mb = ctx.conf.get('bndl_cassandra.part_size_mb')
    max_size_keys = ctx.conf.get('bndl_cassandra.part_size_keys')

    return _partition_ranges(ctx, contact_points, keyspace, max_size_mb, max_size_keys,
                     table, size_estimates)



@lru_cache()
def _partition_ranges(ctx, contact_points, keyspace, max_size_mb, max_size_keys,
                      table, size_estimates):
    assert table or size_estimates
    with cassandra_session(ctx, contact_points=contact_points) as session:
        # estimate size of table
        size_estimate = size_estimates or estimate_size(session, keyspace, table)

        # calculate a maximum length of a partition
        # while the ratio of size in megabytes and keys may vary across the tokens
        # we use the average sizes per token for both to simplify the problem
        # to 1d bin packing (instead of vector bin packing)
        max_length = T_COUNT / ctx.default_pcount
        if size_estimate.token_size_mb:
            max_length = min(max_length, max_size_mb / size_estimate.token_size_mb)
        if size_estimate.token_size_keys:
            max_length = min(max_length, max_size_keys / size_estimate.token_size_keys)

        # get token ranges, grouped by replica set
        by_replicas = ranges_by_replicas(session, keyspace)

    # container for the partitions (replica, ranges, size_mb, size_keys)
    partitions = []

    # divide the token ranges in partitions, joining ranges for the same replica set
    # but limited in size (in bytes and Cassandra partition keys)
    # A greedy bin packing algorithm is used

    for replicas, ranges in sorted(by_replicas.items(), key=itemgetter(0)):
        ranges = itertoolz.pluck([1, 2], ranges)
        partitioned = _partition_replica_ranges(tuple(ranges), max_length, size_estimate)
        for ranges, size in partitioned:
            partitions.append((
                replicas, ranges,
                size * size_estimate.token_size_mb,
                size * size_estimate.token_size_keys
            ))

    # shuffle the partitions so that load is spread over the replica sets
    # use a fixed seed so that the 'randomness' is fixed
    random.Random(42).shuffle(partitions)

    return partitions


def _partition_replica_ranges(ranges, max_length, size_estimate):
    # split ranges such that they are small enough
    # ranges which need to be split are yielded immediately, they'd fill a bin anyway
    # the ranges smaller than max_length are put into a list for binning
    # and sorted larger > smaller (First Fit Decreasing strategy)
    partitioned = []

    sorted_ranges = []
    for start, end in ranges:
        length = end - start
        if length > max_length:
            parts = int((length - 1) / max_length)
            step = math.ceil(length / (parts + 1))
            for _ in range(parts):
                partitioned.append((((start, start + step),), step))
                start = start + step
            partitioned.append((((start, end),), end - start))
        else:
            sorted_ranges.append((start, end, length))
    # Sort biggest first
    sorted_ranges.sort(key=itemgetter(2), reverse=True)

    # container for the bins for this replica set
    bins = [Bin()]

    # build one big partition out of the ranges for this replica set
    for start, end, length in sorted_ranges:
        bin = bins[0]
        # see if it fits in the first (least loaded bin)
        if bin.size + length > max_length:
            # create a new bin if none found
            bin = Bin()
            bins.append(bin)
        # add the token range to the bin
        bin.ranges.append((start, end))
        bin.size += length

        # sort the bins so that the least loaded bin is the first candidate
        bins.sort(key=lambda bin: bin.size)

    for bin in bins:
        if bin.size:
            partitioned.append((bin.ranges, bin.size))

    return partitioned



class SizeEstimate(object):
    def __init__(self, size, partitions, fraction):
        if fraction:
            self.table_size_mb = int(size / fraction)
            self.table_size_pk = int(partitions / fraction)
            self.token_size_mb = float(self.table_size_mb) / T_COUNT
            self.token_size_keys = float(self.table_size_pk) / T_COUNT
        else:
            self.table_size_mb = 0
            self.table_size_pk = 0
            self.token_size_mb = 0
            self.token_size_keys = 0

    def __add__(self, other):
        est = copy.copy(self)
        est += other
        return est

    def __iadd__(self, other):
        self.table_size_mb += other.table_size_mb
        self.table_size_pk += other.table_size_pk
        self.token_size_keys = (self.token_size_keys + other.token_size_keys) / 2
        self.token_size_mb = (self.token_size_mb + other.token_size_mb) / 2
        return self

    def __hash__(self):
        return (hash(self.table_size_mb) ^
                hash(self.table_size_pk) ^
                hash(self.token_size_mb) ^
                hash(self.token_size_keys))

    def __eq__(self, other):
        return (self.table_size_mb == other.table_size_mb and
                self.table_size_pk == other.table_size_pk and
                self.token_size_mb == other.token_size_mb and
                self.token_size_keys == other.token_size_keys)

    def __repr__(self):
        return '<SizeEstimate: size=%s, partitions=%s, partitions / token=%s, token size=%s>' % (
            self.table_size_mb,
            self.table_size_pk,
            self.token_size_keys,
            self.token_size_mb
        )


def estimate_size(session, keyspace, table):
    size_estimate_query = '''
        select range_start, range_end, partitions_count, mean_partition_size
        from system.size_estimates
        where keyspace_name = %s and table_name = %s
    '''
    size_estimates = list(session.execute(size_estimate_query, (keyspace, table)))

    size_b = 0
    size_pk = 0
    tokens = 0

    if len(size_estimates) == 1:
        range_estimate = size_estimates[0]
        size_pk = range_estimate.partitions_count
        size_b = range_estimate.mean_partition_size * range_estimate.partitions_count
        tokens = T_COUNT
    else:
        for range_estimate in size_estimates:
            start, end = int(range_estimate.range_start), int(range_estimate.range_end)
            # don't bother unwrapping the token range crossing 0
            if start > end:
                continue
            # count partitions, bytes and size of the token range
            size_pk += range_estimate.partitions_count
            size_b += range_estimate.mean_partition_size * range_estimate.partitions_count
            tokens += int(range_estimate.range_end) - int(range_estimate.range_start)

    fraction = tokens / T_COUNT

    return SizeEstimate(size_b / 1024 / 1024, size_pk, fraction)
