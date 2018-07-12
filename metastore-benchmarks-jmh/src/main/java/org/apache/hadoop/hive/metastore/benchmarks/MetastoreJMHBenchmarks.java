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

import org.apache.hadoop.hive.metastore.tools.HMSClient;
import org.apache.thrift.TException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
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

import static org.apache.hadoop.hive.metastore.tools.Util.getServerUri;
import static org.apache.hadoop.hive.metastore.tools.Util.throwingSupplierWrapper;


public class MetastoreJMHBenchmarks {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreJMHBenchmarks.class);

  private static final String PROP_DATABASE = "db.name";
  private static final String PROP_TABLE = "table.name";

  private static final String DEFAULT_DB_NAME = "bench_jmh_" + System.getProperty("user.name");
  private static final String DEFAULT_TABLE_NAME = "bench_jmh_table";



  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(MetastoreJMHBenchmarks.class.getSimpleName())
            .forks(1)
            .verbosity(VerboseMode.NORMAL)
            .mode(Mode.AverageTime)
            .build();

    new Runner(opt).run();
  }

  @State(Scope.Thread)
  public static class benchmarkState {
    private String dbName;
    private HMSClient client;
    private String tableName;

    @Setup()
    public void setup() throws TException, IOException, InterruptedException, LoginException, URISyntaxException {
      dbName = System.getProperty(PROP_DATABASE);
      if (dbName == null) {
        dbName = DEFAULT_DB_NAME;
      }
      tableName = System.getProperty(PROP_TABLE);
      if (tableName == null) {
        tableName = DEFAULT_TABLE_NAME;
      }

      client = new HMSClient(getServerUri(null, null));
    }

    @Benchmark
    public void getAllDatabases() {
      throwingSupplierWrapper(() -> client.getAllDatabases(null));
    }
  }
}
