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

package org.apache.hadoop.hive.metastore.benchmarks;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.tools.HMSClient;
import org.apache.hadoop.hive.metastore.tools.Util;
import org.apache.thrift.TException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.metastore.tools.Util.createSchema;
import static org.apache.hadoop.hive.metastore.tools.Util.getServerUri;
import static org.apache.hadoop.hive.metastore.tools.Util.throwingSupplierWrapper;


public class MetastoreJMHBenchmarks {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreJMHBenchmarks.class);

  private static final String PROP_DATABASE = "db.name";
  private static final String PROP_TABLE = "table.name";

  private static final String DEFAULT_DB_NAME = "bench_jmh_" + System.getProperty("user.name");
  private static final String DEFAULT_TABLE_NAME = "bench_jmh_table";

  // Create a simple table with a single column and single partition
  private static void createPartitionedTable(HMSClient client, String dbName, String tableName) {
    throwingSupplierWrapper(() -> client.createTable(
            new Util.TableBuilder(dbName, tableName)
                    .withType(TableType.MANAGED_TABLE)
                    .withColumns(createSchema(Collections.singletonList("name:string")))
                    .withPartitionKeys(createSchema(Collections.singletonList("date")))
                    .build()));
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(MetastoreJMHBenchmarks.class.getSimpleName())
            .forks(1)
            .verbosity(VerboseMode.NORMAL)
            .mode(Mode.AverageTime)
            .build();

    new Runner(opt).run();
  }

  static class benchmarkState {
    String dbName;
    HMSClient client;

    void init() throws TException, URISyntaxException, InterruptedException, LoginException, IOException {
      dbName = System.getProperty(PROP_DATABASE);
      if (dbName == null) {
        dbName = DEFAULT_DB_NAME;
      }
      client = new HMSClient(getServerUri(null, null));
      try {
        client.getDatabase(dbName);
      } catch (Exception e) {
        client.createDatabase(dbName);
      }
    }

    void close() {
      try {
        client.dropDatabase(dbName);
        client.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @State(Scope.Thread)
  public static class benchCreateTable extends benchmarkState {
    private String tableName;
    private Table table;

    @Setup(Level.Trial)
    public void setup() throws TException, IOException, InterruptedException, LoginException, URISyntaxException {
      init();
      tableName = System.getProperty(PROP_TABLE);
      if (tableName == null) {
        tableName = DEFAULT_TABLE_NAME;
      }
      table = Util.TableBuilder.buildDefaultTable(dbName, tableName);
    }

    @TearDown(Level.Trial)
    public void teardown() {
      close();
    }

    @TearDown(Level.Invocation)
    public void cleanup() throws TException {
      client.dropTable(dbName, tableName);
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    public void createTable() throws TException {
      client.createTable(table);
    }
  }

  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @State(Scope.Thread)
  public static class benchDropTable extends benchmarkState {
    private String tableName;
    private Table table;

    @Setup(Level.Trial)
    public void setup() throws TException, IOException, InterruptedException, LoginException, URISyntaxException {
      init();
      tableName = System.getProperty(PROP_TABLE);
      if (tableName == null) {
        tableName = DEFAULT_TABLE_NAME;
      }
      table = Util.TableBuilder.buildDefaultTable(dbName, tableName);
    }

    @TearDown(Level.Trial)
    public void teardown() {
      close();
    }

    @Setup(Level.Invocation)
    public void cleanup() throws TException {
      client.createTable(table);
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    public void dropTableTable() throws TException {
      client.dropTable(dbName, tableName);
    }
  }

  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @State(Scope.Thread)
  public static class benchGetEventId extends benchmarkState {

    @Setup(Level.Trial)
    public void setup() throws TException, IOException, InterruptedException, LoginException, URISyntaxException {
      init();
    }

    @TearDown(Level.Trial)
    public void teardown() {
      close();
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    public void getCurrentEventId() throws TException {
      client.getCurrentNotificationId();
    }
  }

  static class partitionedTableState extends benchmarkState {
    String tableName;

    @Override
    void init() throws InterruptedException, IOException, TException, LoginException, URISyntaxException {
      super.init();
      tableName = System.getProperty(PROP_TABLE);
      if (tableName == null) {
        tableName = DEFAULT_TABLE_NAME;
      }
      createPartitionedTable(client, dbName, tableName);
    }

    @Override
    void close() {
      try {
        client.dropTable(dbName, tableName);
      } catch (TException e) {
        e.printStackTrace();
      }
      super.close();
    }
  }

  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @State(Scope.Thread)
  public static class benchGetTable extends partitionedTableState {

    @Setup(Level.Trial)
    public void setup() throws TException, IOException, InterruptedException, LoginException, URISyntaxException {
      init();
    }

    @TearDown(Level.Trial)
    public void teardown() {
      close();
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    public void getTable() throws TException {
      client.getTable(dbName, tableName);
    }
  }
}
