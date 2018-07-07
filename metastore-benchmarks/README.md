## Hive Metastore micro-benchmarks

## Installation

    mvn clean install

You can run tests as well. Just set `HMS_HOST` environment variable to some HMS instance which is
capable of running your requests (non-kerberised one) and run

    mvn install
    
target directory has two mega-jars which have all the dependencies.

Alternatively you can use [bin/hbench](../bin/hbench) script which use Maven to run the code.

## HmsBench usage

    Usage: BenchmarkTool [-ChlV] [--sanitize] [--confdir=<confDir>]
                         [--params=<nParameters>] [--savedata=<dataSaveDir>]
                         [--separator=<csvSeparator>] [-d=<dbName>] [-H=URI]
                         [-L=<spinCount>] [-N=<instances>] [-o=<outputFile>]
                         [-P=<port>] [-t=<tableName>] [-T=<nThreads>] [-W=<warmup>]
                         [-E=<exclude>]... [-M=<matches>]...
          --confdir=<confDir>    configuration directory
          --params=<nParameters> number of table/partition parameters
                                   Default: 0
          --sanitize             sanitize results (remove outliers)
          --savedata=<dataSaveDir>
                                 save raw data in specified dir
          --separator=<csvSeparator>
                                 CSV field separator
                                   Default: 	
      -C, --csv                  produce CSV output
      -d, --db=<dbName>          database name
      -E, --exclude=<exclude>    test name patterns to exclude
      -h, --help                 Show this help message and exit.
      -H, --host=URI             HMS Host
      -l, --list                 list matching benchmarks
      -L, --spin=<spinCount>     spin count
                                   Default: 100
      -M, --pattern=<matches>    test name patterns
      -N, --number=<instances>   umber of object instances
                                   Default: 100
      -o, --output=<outputFile>  output file
      -P, --port=<port>          HMS Server port
                                   Default: 9083
      -t, --table=<tableName>    table name
      -T, --threads=<nThreads>   number of concurrent threads
                                   Default: 2
      -V, --version              Print version information and exit.
      -W, --warmup=<warmup>      warmup count
                                   Default: 15
                               
### Using single jar

    java -jar hbench-jar-with-dependencies.jar <optins> [test]...

### Using hbench on kerberized cluster

    java -jar hbench-jar-with-dependencies.jar -H `hostname` <optins> [test]...
    
### Examples

1. Run tests with 500 objects created, 10 times warm-up and exclude concurrent operations and drop operations

    java -jar hbench-jar-with-dependencies.jar -H `hostname` -N 500 -W 10 -E 'drop.*' -E 'concurrent.*'
    
2. Run tests, produce output in tab-separated format and write individual data points in 'data' directory
  
    java -jar hbench-jar-with-dependencies.jar -H host.com -o result.csv --csv --savedata data