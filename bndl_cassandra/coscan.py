from functools import partial, lru_cache
import logging

from bndl.compute.dataset import Dataset, Partition
from bndl.util.funcs import identity, getter
from bndl_cassandra import partitioner
from bndl_cassandra.partitioner import estimate_size, SizeEstimate
from bndl_cassandra.session import cassandra_session
from cytoolz.itertoolz import take


logger = logging.getLogger(__name__)


def get_or_none(index, container):
    try:
        return container[index]
    except IndexError:
        return None


class CassandraCoScanDataset(Dataset):
    def __init__(self, *scans, keys=None, dset_id=None):
        assert len(scans) > 1
        scans = list(scans)
        scan0 = scans[0]

        super().__init__(scan0.ctx, src=scans, dset_id=dset_id)

        for idx, scan in enumerate(scans[1:], 1):
            if isinstance(scan, str):
                if '.' in scan:
                    keyspace, table = scan.split('.', 1)
                else:
                    keyspace, table = scan0.keyspace, scan
                scan = self.ctx.cassandra_table(keyspace, table)
                scans[idx] = scan
            assert scan.contact_points == scan0.contact_points, "only scan in parallel within the same cluster"
            assert scan.keyspace == scan0.keyspace, "only scan in parallel within the same keyspace"
        assert len(set(scan.table for scan in scans)) == len(scans), "don't scan the same table twice"

        self.contact_points = scan0.contact_points
        self.keyspace = scan0.keyspace

        self.srcparts = [src.parts() for src in scans]
        self.pcount = len(self.srcparts[0])

        # TODO check format (dicts, tuples, namedtuples, etc.)
        # TODO adapt keyfuncs to below

        if isinstance(keys, str):
            self.keys = keys = [keys] * len(scans)
        else:
            self.keys = keys

        with cassandra_session(self.ctx, contact_points=self.contact_points) as session:
            ks_meta = session.cluster.metadata.keyspaces[self.keyspace]
            tbl_metas = [ks_meta.tables[scan.table] for scan in scans]

            if not keys:
                primary_key_length = len(tbl_metas[0].primary_key)
                for tbl_meta in tbl_metas[1:]:
                    assert len(tbl_meta.primary_key) == primary_key_length, \
                        "can't co-scan without keys with varying primary key length"

                self.keyfuncs = [partial(take, primary_key_length)] * len(scans)
                self.grouptransforms = [partial(get_or_none, 0)] * len(scans)

            else:
                assert len(keys) == len(scans), \
                    "provide a key for each table scanned or none at all"

                self.keyfuncs = []
                self.grouptransforms = []
                for key, scan, tbl_meta in zip(keys, scans, tbl_metas):
                    if isinstance(key, str):
                        key = (key,)
                    keylen = len(key)

                    assert len(tbl_meta.partition_key) <= keylen, \
                        "can't co-scan over a table keyed by part of the partition key"
                    assert tuple(key) == tuple(c.name for c in tbl_meta.primary_key)[:keylen], \
                        "the key columns must be the first part (or all) of the primary key"
                    assert scan._select is None or tuple(key) == tuple(scan._select)[:keylen], \
                        "select all columns or the primary key columns in the order as they " \
                        "are defined in the CQL schema"

                    self.keyfuncs.append(partial(take, keylen))
                    if keylen == len(tbl_meta.primary_key):
                        self.grouptransforms.append(partial(get_or_none, 0))
                    else:
                        self.grouptransforms.append(identity)



    def coscan(self, *others, keys=None):
        assert len(others) > 0
        return CassandraCoScanDataset(*tuple(self.src) + others, keys=keys)


    @lru_cache()
    def parts(self):
        from bndl_cassandra.dataset import CassandraScanPartition

        with cassandra_session(self.ctx, contact_points=self.contact_points) as session:
            size_estimates = sum((estimate_size(session, self.keyspace, src.table) for src in self.src),
                                 SizeEstimate(0, 0, 0))

        partitions = partitioner.partition_ranges(self.ctx, self.contact_points, self.keyspace, size_estimates=size_estimates)

        return [
            CassandraCoScanPartition(self, idx, [CassandraScanPartition(scan, idx, *part)
                                                     for scan in self.src])
            for idx, part in enumerate(partitions)
        ]


class CassandraCoScanPartition(Partition):
    def __init__(self, dset, idx, scans):
        super().__init__(dset, idx)
        self.scans = scans


    def _locality(self, workers):
        return self.scans[0]._locality(workers)


    def _compute(self):
        keyfuncs = self.dset.keyfuncs
        grouptransforms = self.dset.grouptransforms

        subscans = [scan.compute() for scan in self.scans]
        merged = {}

        for cidx, scan in enumerate(subscans):
            keyf = keyfuncs[cidx]
            for row in scan:
                key = tuple(keyf(row))
                batch = merged.get(key)
                if not batch:
                    merged[key] = batch = [[] for _ in subscans]
                batch[cidx].append(row)

        for key, groups in merged.items():
            for idx, (group, transform) in enumerate(zip(groups, grouptransforms)):
                groups[idx] = transform(group)
            yield key, groups
