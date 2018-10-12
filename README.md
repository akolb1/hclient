# Collection of tools talking to Hive Metastore over Thrift

## metastore-cli

metastore-cli provides simple Hive metastore client cli interface. It talks directly to HMS without
using Beeline.

See [metastore-cli](metastore-cli/README.md)

## Microbenchmarks

metastore-benchmarks provides a set of simple microbenchmarks for Hive Metastore Thrift calls.

See [metastore-benchmarks](metastore-benchmarks/README.md)

## Common libraries

All common code lives in [tools-common](tools-common). The code implements its own simple
 metastore client and doesn't use 
[HiveMetastoreClient](https://hive.apache.org/javadocs/r2.1.1/api/org/apache/hadoop/hive/metastore/HiveMetaStoreClient.html)
 from Hive distribution.
 
See public [Documentation](https://akolb1.github.io/hclient).

## Installation

    mvn clean install
    
You can also create full jars with dependencies by selecting _dist_ profile:

    mvn clean install -Pdist
    
## Using in Kerberos environment

Use `kinit` to login as user you want to run as:

    kinit -kt /path/tokeytab -l 24h

Make sure you run on a host that has correct HMS configuration in /etc/hive/conf.


## Changing logging level

To change the logging level you need to modify file [log4j2.xml](metastore-benchmarks/src/main/resources/log4j2.xml)
