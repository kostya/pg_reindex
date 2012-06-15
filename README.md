Pg Reindex
==========

Console utility for gracefully rebuild indexes/pkeys for PostgreSQL, with minimal locking in semi-auto mode.

Install:

    $ gem install pg_reindex
  
Using:

    export PGRE_CFG=/some/path/database.yml 
    pgre --help

User in connection should be an owner of relations. And for rebuild pkey, should be superuser.

Tasks:

    pgre dbs                                        # Show list of databases from database.yml
    pgre install ENV                                # Install function swap_for_pkey to database
    pgre rebuild ENV (table|index)[,(table|index)]  # rebuild tables,indexes,pkeys
    pgre tables ENV                                 # Show tables of database

Use case:
  
    # Show databases
    pgre dbs

    # Show tables with indexes, which full size more than 1Gb
    pgre tables production -s 1000

    # Show process without really rebuild
    pgre rebuild production users,some_index1
  
    # Rebuild indexes 
    pgre rebuild production users --write
    
    # Rebuild indexes and no ask confirmation (not recommended)
    pgre rebuild production users --write --no-ask  


Explanation/Warning:
--------------------

Rebuild index produces sqls:
    
    1. CREATE INDEX CONCURRENTLY bla2 on some_table USING btree (some_field);
    2. ANALYZE some_table;
    3. DROP INDEX bla;
    4. ALTER INDEX bla2 RENAME TO bla; 
    
1 can be blocked by long running query(LRQ), or autovacuum (in this case kill autovacuum or wait LRQ).
**By careful**, if between 1 and (3,4) started LRQ or autovacuum, in this case (3,4) will block all queries on this table. 
If it happens, and (3,4) not quit after < 30s, you should stop (3,4) by cancel query in PostgreSQL, and later execute manually.
  
  
Rebuild pkey produces sqls:
  
    1. CREATE UNIQUE INDEX CONCURRENTLY some_table_pkey2 on some_table USING btree (ID);
    2. ANALYZE some_table;
    3. SELECT swap_for_pkey('public', 'some_table_pkey', 'some_table_pkey2');

  Same issue with 1 and 3.


MIT-LICENCE
