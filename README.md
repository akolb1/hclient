Simple CLI client for HMS Metastore.

#Usage

    usage: hclient list|create|addpart <options> [name:type...]
     -conf <arg>             configuration directory
     -d,--database <arg>     database name (can be regexp for list)
     -D,--drop               drop table if exists
     -H,--host <arg>         HMS Server
     -h,--help               print this info
     -N,--number <arg>       number of instances
     -P,--partitions <arg>   partitions list
     -S,--pattern <arg>      table name pattern for bulk creation
     -showparts              show partitions
     -t,--table <arg>        table name (can be regexp for list)
     -v,--verbose            verbose mode


     usage: hbench ...
      -conf <arg>             configuration directory
      -csv                    produce CSV output
      -d,--database <arg>     database name (can be regexp for list)
      -H,--host <arg>         HMS Server
      -h,--help               print this info
      -K,--separator <arg>    field separator
      -L,--spin <arg>         spin count
      -l,--list <arg>         list benchmarks
      -N,--number <arg>       number of instances
      -o,--output <arg>       output file
      -P,--partitions <arg>   partitions list
      -S,--pattern <arg>      test patterns
      -sanitize               sanitize results
      -savedata <arg>         save raw data in specified dir
      -v,--verbose            verbose mode
      -W,--warm <arg>         warmup count

     

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
