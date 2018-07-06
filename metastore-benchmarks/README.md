## Hive Metastore micro-benchmarks

## Installation

    mvn clean install

You can run tests as well. Just set `HMS_HOST` environment variable to some HMS instance which is
capable of running your requests (non-kerberised one) and run

    mvn install
    
target directory has two mega-jars which have all the dependencies.

Alternatively you can use [bin/hbench](../bin/hbench) script which use Maven to run the code.

## HmsBench usage

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

### Using single jar

    java -jar hbench-jar-with-dependencies.jar <optins> [test]...

### Using hbench on kerberized cluster

    java -jar hbench-jar-with-dependencies.jar -H `hostname` <optins> [test]...
    
### Examples

1. Run tests with 500 objects created, 10 times warm-up and exclude concurrent operations and drop operations

    java -jar hbench-jar-with-dependencies.jar -H `hostname` -N 500 -W 10 !drop.* !concurrent.*
    
2. Run tests, produce output in tab-separated format and write individual data points in 'data' directory
  
    java -jar hbench-jar-with-dependencies.jar -H host.com -o result.csv -csv -savedata data