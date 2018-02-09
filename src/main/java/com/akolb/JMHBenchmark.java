/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.akolb;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import static com.akolb.Util.addManyPartitions;
import static com.akolb.Util.createSchema;
import static com.akolb.Util.getServerUri;

@State(Scope.Thread)
public class JMHBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(JMHBenchmark.class);

  private static final String TEST_TABLE = "bench_table1";
  private static final int NOBJECTS = 1000;
  private static final String PROP_HOST = "hms.host";
  private static final String PROP_PORT = "hms.port";
  private static final String PROP_DATABASE = "db.name";
  private static final String PROP_TABLE = "table.name";


  private HMSClient client;
  private String dbName;
  private String tableName;
  List<FieldSchema> tableSchema;
  List<FieldSchema> partitionSchema;
  private Table table;

  public static void main(String[] args) throws RunnerException, TException {

    HMSClient client = null;
    String host = System.getProperty(PROP_HOST);
    Integer port = Integer.getInteger(PROP_PORT);
    if (host == null) {
      LOG.error("Missing hostname");
      System.exit(1);
    }


    LOG.info("host = {}, port = {}", host, port);
    try {
      client = new HMSClient(getServerUri(host, port));
    } catch (IOException e) {
      LOG.error("Failed to connect to HMS", e);
      System.exit(1);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while connecting to HMS", e);
      System.exit(1);
    } catch (LoginException e) {
      LOG.error("Failed to perform Kerberos login", e);
    } catch (URISyntaxException e) {
      LOG.error("Invalid URI syntax", e);
    }
    String dbName = System.getProperty(PROP_DATABASE);
    String tableName = System.getProperty(PROP_TABLE);

    if (dbName == null || dbName.isEmpty()) {
      throw new RuntimeException("Missing DB name");
    }
    if (tableName == null || tableName.isEmpty()) {
      throw new RuntimeException("Missing Table name");
    }

    LOG.info("Using table '{}.{}'", dbName, tableName);

    if (!client.dbExists(dbName)) {
      client.createDatabase(dbName);
    }

    if (client.tableExists(dbName, tableName)) {
      client.dropTable(dbName, tableName);
    }

    Options opt = new OptionsBuilder()
        .include(JMHBenchmark.class.getSimpleName())
        .forks(1)
        .verbosity(VerboseMode.NORMAL)
        .mode(Mode.AverageTime)
        .build();

    new Runner(opt).run();
  }

  @Setup
  public void setup() throws TException, IOException, InterruptedException, LoginException, URISyntaxException {
    Logger LOG = LoggerFactory.getLogger(JMHBenchmark.class);
    tableName = System.getProperty(PROP_TABLE);
    dbName = System.getProperty(PROP_DATABASE);
    String server = System.getProperty(PROP_HOST);
    LOG.info("Using server " + server + " table '" + dbName + "." + tableName + "'");
    client = new HMSClient(getServerUri(server, Integer.getInteger(PROP_PORT)));
    table = Util.TableBuilder.buildDefaultTable(dbName, tableName);
    LOG.info("Create partitioned table {}.{}", dbName, TEST_TABLE);
    client.createTable(
        new Util.TableBuilder(dbName, TEST_TABLE)
            .setColumns(createSchema(Collections.singletonList("name:string")))
            .setPartitionKeys(createSchema(Collections.singletonList("date")))
            .build());
  }

  @TearDown
  public void teardown() throws Exception {
    Logger LOG = LoggerFactory.getLogger(JMHBenchmark.class);
    LOG.info("dropping table {}.{}", dbName, TEST_TABLE);
    client.dropTable(dbName, TEST_TABLE);
  }

  @Benchmark
  public void createTable() throws TException {
    client.createTable(table);
    client.dropTable(dbName, tableName);
  }

  @Benchmark
  public void getAllDatabases() {
    client.getAllDatabasesNoException();
  }

  @Benchmark
  public void getAllTables() {
    client.getAllTablesNoException(dbName);
  }

  @Benchmark
  public void createDropPartitions() {
    try {
      addManyPartitions(client, dbName, TEST_TABLE,
          Collections.singletonList("d"), NOBJECTS);
      client.dropPartitions(dbName, TEST_TABLE, null);
    } catch (TException e) {
      e.printStackTrace();
    }
  }

}
