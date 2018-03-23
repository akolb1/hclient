Simple CLI client for HMS Metastore.

# Hclient Usage

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



# Examples

    $ export HMS_=HOST=host.domain.com
    
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

# HmsBench usage

     usage: hbench <options> [test]...
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
     -P,--port <arg>         HMS Server port
     -p,--partitions <arg>   partitions list
     -S,--pattern <arg>      test patterns
     -sanitize               sanitize results
     -savedata <arg>         save raw data in specified dir
     -T,--threads <arg>      numberOfThreads
     -v,--verbose            verbose mode
     -W,--warm <arg>         warmup count

## Using single jar

    java -jar hbench-jar-with-dependencies.jar <optins> [test]...

## Using hbench on kerberized cluster

    java -jar hbench-jar-with-dependencies.jar -H `hostname` <optins> [test]...
    
## Examples

1. Run tests with 500 objects created, 10 times warm-up and exclude concurrent operations and drop operations

    java -jar hbench-jar-with-dependencies.jar -H `hostname` -N 500 -W 10 !drop.* !concurrent.*
    
2. Run tests, produce output in tab-separated format and write individual data points in 'data' directory

    
    java -jar hbench-jar-with-dependencies.jar -H host.com -o result.csv -csv -savedata data