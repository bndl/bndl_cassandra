from collections import defaultdict
from datetime import timedelta, date, datetime
from functools import partial
from threading import Condition
import functools
import logging

from bndl.util.retry import retry_delay
from bndl.util.timestamps import ms_timestamp
from bndl_cassandra.session import cassandra_session, TRANSIENT_ERRORS
from cassandra.query import BatchStatement, BatchType, BoundStatement


logger = logging.getLogger(__name__)


INSERT_TEMPLATE = (
    'insert into {keyspace}.{table} '
    '({columns}) values ({placeholders})'
    '{using}'
)


def batch_statements(session, key, batch_size, buffer_size, statements):
    from bndl_cassandra import BatchKey
    if key == BatchKey.none or key is None:
        yield from statements
        return
    elif key == BatchKey.replica_set:
        get_replicas = session.cluster.metadata.get_replicas
        key = lambda stmt: ''.join(replica.address for replica in get_replicas(stmt.keyspace, stmt.routing_key))
    else:
        key = lambda stmt: stmt.routing_key

    statements = iter(statements)

    by_key = {}

    def create_batch_statement(statements):
        first = statements[0]
        batch = BatchStatement(BatchType.UNLOGGED, first.retry_policy,
                            first.consistency_level,
                            first.serial_consistency_level)
        for stmt in statements:
            batch.add(stmt)
        return batch

    def create_batch(stmt, k):
        batch = BatchStatement(BatchType.UNLOGGED,
                       stmt.retry_policy,
                       stmt.consistency_level,
                       stmt.serial_consistency_level)
        by_key[k] = batch
        # The Java driver has the option to request the encoded size, the
        # python driver doesn't, this is an approximation of the size based
        # on the v4 Cassandra protocol
        # batch type is a byte, no queries is 2 bytes, consistency level is a
        # short (2 bytes), flags is a byte, timestamp is 8 bytes
        # adding 6 bytes margin makes 20
        batch.size = 20
        return batch

    while True:
        try:
            stmt = next(statements)
        except StopIteration:
            yield from by_key.values()
            break
        else:
            k = key(stmt)
            batch = by_key.get(k)
            # The Java driver has the option to request the encoded size, the
            # python driver doesn't, this is an approximation of the size based
            # on the v4 Cassandra protocol
            # no params is 2 bytes, statement type is a byte
            # add some 5 bytes margin makes 8
            # each value is prepended by an int
            stmt_size = 8 + 4 * len(stmt.values) + \
                        sum(len(v) for v in stmt.values if type(v) == bytes)
            if isinstance(stmt, BoundStatement):
                # query strings are prepended by a long
                stmt_size += 8 + len(stmt.prepared_statement.query_id)
            else:
                # query ids are prepended by a short
                stmt_size += 2 + len(stmt.query_string)

            if batch is None:
                batch = create_batch(stmt, k)
            elif batch.size + stmt_size > batch_size:
                yield batch
                batch = create_batch(stmt, k)
            batch.add(stmt)
            batch.size += stmt_size


def execute_save(ctx, statement, iterable, keyspace=None, contact_points=None):
    '''
    Save elements from an iterable given the insert/update query. Use
    cassandra_save to save a dataset. This method is useful when saving
    to multiple tables from a single dataset with map_partitions.

    :param ctx: bndl.compute.context.ComputeContext
        The BNDL compute context to use for configuration and accessing the
        cassandra_session.
    :param statement: str
        The Cassandra statement to use in saving the iterable.
    :param iterable: list, tuple, generator, iterable, ...
        The values to save.
    :param keyspace: str or None
        The keyspace to execute the save in (if the statements don't have an
        explicit keyspace). keyspace is supplied to ctx.cassandra_session which
        defaults to using the 'bndl_cassandra.keyspace' setting as default value.
    :param contact_points: str, tuple, or list
        A string or tuple/list of strings denoting host names (contact points)
        of the Cassandra cluster to save to. Defaults to using the ip addresses
        in the BNDL cluster.
    :return: A count of the records saved.
    '''
    consistency_level = ctx.conf.get('bndl_cassandra.write_consistency_level')
    timeout = ctx.conf.get('bndl_cassandra.write_timeout')
    retry_count = ctx.conf.get('bndl_cassandra.write_retry_count')
    retry_backoff = ctx.conf.get('bndl_cassandra.write_retry_backoff')
    concurrency = max(1, ctx.conf.get('bndl_cassandra.write_concurrency'))

    batch_key = ctx.conf.get('bndl_cassandra.write_batch_key')
    batch_size = ctx.conf.get('bndl_cassandra.write_batch_size') * 1024
    batch_buffer_size = ctx.conf.get('bndl_cassandra.write_batch_buffer_size')

    if logger.isEnabledFor(logging.INFO):
        logger.info('executing cassandra save with statement %s', statement.replace('\n', ''))

    with cassandra_session(ctx, keyspace=keyspace, contact_points=contact_points) as session:
        prepared_statement = session.prepare(statement)
        prepared_statement.consistency_level = consistency_level

        # bind each element in iterable to the prepared statement
        statements = map(prepared_statement.bind, iterable)
        # batching statements as configured
        statements = batch_statements(session, batch_key, batch_size,
                                      batch_buffer_size, statements)

        saved = 0
        pending = 0
        cond = Condition()

        failure = None
        failcounts = defaultdict(int)

        def on_done(results, idx, statement):
            nonlocal saved, pending, cond
            with cond:
                if isinstance(statement, BatchStatement):
                    saved += len(statement._statements_and_parameters)
                else:
                    saved += 1
                pending -= 1
                cond.notify_all()

        def on_failed(exc, idx, statement):
            nonlocal failcounts, session
            if type(exc) in TRANSIENT_ERRORS and failcounts[idx] < retry_count:
                failcounts[idx] += 1
                if retry_backoff:
                    sleep = retry_delay(retry_backoff, failcounts[idx])
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug('query failed with %s, %s in total for this query, backing off with sleep of %ss',
                                     type(exc).__name__, failcounts[idx], sleep)
                    session.cluster.scheduler.schedule(sleep, partial(exec_async, idx, statement))
                else:
                    exec_async(idx, statement)
            else:
                nonlocal failure, pending, cond
                with cond:
                    failure = exc
                    pending -= 1
                    cond.notify_all()

        def exec_async(idx, statement):
            nonlocal session
            future = session.execute_async(statement, timeout=timeout)
            future.add_callback(on_done, idx, statement)
            future.add_errback(on_failed, idx, statement)

        for idx, statement in enumerate(statements):
            with cond:
                cond.wait_for(lambda: pending < concurrency)
                if failure:
                    raise failure
                pending += 1
            exec_async(idx, statement)

        if failure:
            raise failure

        logger.debug('all queries issued, waiting for completion')

        with cond:
            cond.wait_for(lambda: pending <= 0)

        if failure:
            raise failure

    return (saved,)


def cassandra_save(dataset, keyspace, table, columns=None, keyed_rows=True,
                   ttl=None, timestamp=None, contact_points=None):
    '''
    Performs a Cassandra insert for each element of the dataset.

    :param dataset: bndl.compute.context.ComputationContext
        As the cassandra_save function is typically bound on ComputationContext
        this corresponds to self.
    :param keyspace: str
        The name of the Cassandra keyspace to save to.
    :param table:
        The name of the Cassandra table (column family) to save to.
    :param columns:
        The names of the columns to save.

        When the dataset contains tuples, this arguments is the mapping of the
        tuple elements to the columns of the table saved to. If not provided,
        all columns are used.

        When the dataset contains dictionaries, this parameter limits the
        columns save to Cassandra.
    :param keyed_rows: bool
        Whether to expect a dataset of dicts (or at least supports the
        __getitem__ protocol with columns names as key) or positional objects
        (e.g. tuples or lists).
    :param ttl: int
        The time to live to use in saving the records.
    :param timestamp: int or datetime.date or datetime.datetime
        The timestamp to use in saving the records.
    :param contact_points: str, tuple, or list
        A string or tuple/list of strings denoting hostnames (contact points)
        of the Cassandra cluster to save to. Defaults to using the ip addresses
        in the BNDL cluster.

    Example:

        >>> ctx.collection([{'key': 1, 'val': 'a' }, {'key': 2, 'val': 'b' },]) \
               .cassandra_save('keyspace', 'table') \
               .execute()
        >>> ctx.range(100) \
               .map(lambda i: {'key': i, 'val': str(i) } \
               .cassandra_save('keyspace', 'table') \
               .sum()
        100
    '''
    if ttl or timestamp:
        using = []
        if ttl:
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds() * 1000)
            using.append('ttl ' + str(ttl))
        if timestamp:
            if isinstance(timestamp, (date, datetime)):
                timestamp = ms_timestamp(timestamp)
            using.append('timestamp ' + str(timestamp))
        using = ' using ' + ' and '.join(using)
    else:
        using = ''

    if not columns:
        with dataset.ctx.cassandra_session(contact_points=contact_points) as session:
            table_meta = session.cluster.metadata.keyspaces[keyspace].tables[table]
            columns = list(table_meta.columns)

    placeholders = (','.join(
        (':' + c for c in columns)
        if keyed_rows else
        ('?' for c in columns)
    ))

    insert = INSERT_TEMPLATE.format(
        keyspace=keyspace,
        table=table,
        columns=', '.join(columns),
        placeholders=placeholders,
        using=using,
    )

    do_save = functools.partial(execute_save, dataset.ctx, insert, contact_points=contact_points)
    return dataset.map_partitions(do_save)
