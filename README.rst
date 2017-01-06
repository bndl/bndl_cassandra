==============
BNDL Cassandra
==============

BNDL Cassandra exposes loading from and saving to functionality of the
`Datastax python driver <https://github.com/datastax/python-driver>`_ for
`Apache Cassandra <http://cassandra.apache.org/>`_.

Master branch build status: |travis| |codecov|

.. |travis| image:: https://travis-ci.org/bndl/cassandra.svg?branch=master
   :target: https://travis-ci.org/bndl/cassandra

.. |codecov| image:: https://codecov.io/gh/bndl/cassandra/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/bndl/cassandra/branch/master

---------------------------------------------------------------------------------------------------

BNDL Cassandra can be installed through pip::

    pip install bndl-cassandra

The main features of BNDL Cassandra are loading from and saving to Cassandra::

    dataset = ctx.cassandra_table(keyspace, table)
    dataset.cassandra_save(keyspace2, table2).execute()
    
    
Furthermore it allows you to join a BNDL dataset with Cassandra::
   
   dataset.join_with_cassandra(keyspace, table).on(columns=columns, keys=keys).left().count()
