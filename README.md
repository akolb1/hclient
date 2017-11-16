Simple CLI client for HMS Metastore.

#Usage

     usage: hclient list|create <options> [name:type...]
      -d,--database <arg>     database name (can be regexp for list)
      -D,--drop               drop table if exists
      -h,--help               print this info
      -N,--number <arg>       number of instances
      -p,--port <arg>         port
      -P,--partitions <arg>   partitions list
      -s,--server <arg>       HMS Server
      -S,--pattern <arg>      table name pattern for bulk creation
      -t,--table <arg>        table name (can be regexp for list)
      -v,--verbose            verbose mode

     
     

# Examples

    $ export HMS_THRIFT_SERVER=host.domain.com
    
## List all databases and tables

    $ hclinent list
    test.foo
    test.bar
    default.customers
    default.impala_parquet_timestamps
    default.impala_timestamps
    default.sample_07
    default.sample_08
    
## List all tables in default database

    $ hclient list -d default
    default.customers
    default.impala_parquet_timestamps
    default.impala_timestamps
    default.sample_07
    default.sample_08
    default.web_logs
    default.web_logs1
    
## List all tables with name 'impala'

    $ hclient list -d default -t '.*impala.*'
    default.impala_parquet_timestamps
    default.impala_timestamps

## List table schemas for impala tables

    $ hclient list -d default -t '.*impala.*' -v
    default.impala_parquet_timestamps
        ts:     timestamp

    default.impala_timestamps
        ts:     timestamp

## Create new table

    $ hclient create -d test_db -t test_table id:int name
    test_db.test_table
            id:     int
            name:   string

## Create table with partitions

    $ hclient create -d test_db -t test_table1 -P date,country id:int name 
    test_db.test_table1
            id:     int
            name:   string
              date: string
              country:      string

## Create multiple tables at once
    $ hclient create -d test_db -t test_table2 -N 3 id:int name -v
    test_db.test_table2_1
            id:     int
            name:   string
    
    test_db.test_table2_2
            id:     int
            name:   string
    
    test_db.test_table2_3
            id:     int
            name:   string
