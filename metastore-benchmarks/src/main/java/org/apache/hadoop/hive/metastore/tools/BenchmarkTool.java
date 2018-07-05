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

package org.apache.hadoop.hive.metastore.tools;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hive.metastore.tools.Constants.OPT_CONF;
import static org.apache.hadoop.hive.metastore.tools.Constants.OPT_DATABASE;
import static org.apache.hadoop.hive.metastore.tools.Constants.OPT_HOST;
import static org.apache.hadoop.hive.metastore.tools.Constants.OPT_PORT;
import static org.apache.hadoop.hive.metastore.tools.Constants.OPT_VERBOSE;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkConcurrentPartitionOps;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkCreatePartition;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkCreatePartitions;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkDeleteCreate;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkDeleteWithPartitions;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkDropDatabase;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkDropPartition;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkDropPartitions;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkGetNotificationId;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkGetPartitionNames;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkGetPartitions;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkGetPartitionsByName;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkGetTable;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkListAllTables;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkListDatabases;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkListManyPartitions;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkListPartition;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkListTables;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkRenameTable;
import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.benchmarkTableCreate;
import static org.apache.hadoop.hive.metastore.tools.Util.getServerUri;

/**
 * BenchmarkTool is a top-level application for measuring Hive Metastore
 * performance.
 */
final class BenchmarkTool {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTool.class);
  private static final TimeUnit scale = TimeUnit.MILLISECONDS;
  private static final String CSV_SEPARATOR = "\t";
  private static final String TEST_TABLE = "bench_table";

  private static final String OPT_SEPARATOR = "separator";
  private static final String OPT_SPIN = "spin";
  private static final String OPT_WARM = "warm";
  private static final String OPT_LIST = "list";
  private static final String OPT_SANITIZE = "sanitize";
  private static final String OPT_OUTPUT = "output";
  private static final String OPT_CSV = "csv";
  private static final String OPT_SAVEDATA = "savedata";
  private static final String OPT_THREADS = "threads";
  private static final String OPT_NPARAMS = "params";
  private static final String OPT_NUMBER = "number";
  private static final String OPT_PATTERN = "pattern";



  // There is no need to instantiate BenchmarkTool class.
  private BenchmarkTool() {}

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("H", OPT_HOST, true,
		      "HMS Server (can also be specified with HMS_HOST environment variable)")
        .addOption("P", OPT_PORT, true, "HMS Server port")
        .addOption("h", "help", false, "print this info")
        .addOption("d", OPT_DATABASE, true, "database name (can be regexp for list)")
        .addOption("v", OPT_VERBOSE, false, "verbose mode")
        .addOption("N", OPT_NUMBER, true, "number of instances")
        .addOption("K", OPT_SEPARATOR, true, "field separator")
        .addOption("L", OPT_SPIN, true, "spin count")
        .addOption("W", OPT_WARM, true, "warmup count")
        .addOption("l", OPT_LIST, false, "list benchmarks")
        .addOption("o", OPT_OUTPUT, true, "output file")
        .addOption("T", OPT_THREADS, true, "numberOfThreads")
        .addOption(new Option(OPT_CONF, true, "configuration directory"))
        .addOption(new Option(OPT_SANITIZE, false, "sanitize results"))
        .addOption(new Option(OPT_CSV, false, "produce CSV output"))
        .addOption(new Option(OPT_NPARAMS, true, "number of parameters"))
        .addOption(new Option(OPT_SAVEDATA, true,
            "save raw data in specified dir"))
        .addOption("S", OPT_PATTERN, true, "test patterns");

    CommandLineParser parser = new DefaultParser();

    CommandLine cmd = null;

    LOG.info("using args {}", Joiner.on(' ').join(args));

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      help(options);
    }

    if (cmd.hasOption("help")) {
      help(options);
    }

    boolean doList = cmd.hasOption(OPT_LIST);

    PrintStream output = System.out;
    if (cmd.hasOption(OPT_OUTPUT)) {
      output = new PrintStream(cmd.getOptionValue(OPT_OUTPUT));
    }

    String dbName = cmd.getOptionValue(OPT_DATABASE);
    String tableName = TEST_TABLE;

    if (!doList && (dbName == null || dbName.isEmpty())) {
      throw new RuntimeException("Missing DB name");
    }

    List<String> arguments = cmd.getArgList();

    boolean filtertests = cmd.hasOption(OPT_PATTERN);
    List<String> patterns = filtertests ?
        Lists.newArrayList(cmd.getOptionValue(OPT_PATTERN).split(",")) :
        Collections.emptyList();
    // If we have arguments, they are filters on the tests, so add them.
    if (!arguments.isEmpty()) {
      patterns = Stream.concat(patterns.stream(), arguments.stream()).collect(Collectors.toList());
    }

    int instances = Integer.parseInt(cmd.getOptionValue(OPT_NUMBER, "100"));
    int nThreads =  Integer.parseInt(cmd.getOptionValue(OPT_THREADS, "2"));
    int warmup = Integer.parseInt(cmd.getOptionValue(OPT_WARM, "15"));
    int spin = Integer.parseInt(cmd.getOptionValue(OPT_SPIN, "100"));
    int nparams = Integer.parseInt(cmd.getOptionValue(OPT_NPARAMS, "1"));
    LOG.info("Using " + instances + " object instances" + " warmup " + warmup +
        " spin " + spin + " nparams " + nparams + " threads " + nThreads);

    StringBuilder sb = new StringBuilder();
    Formatter fmt = new Formatter(sb);
    BenchData bData = new BenchData(dbName, tableName);

    MicroBenchmark bench = new MicroBenchmark(warmup, spin);
    BenchmarkSuite suite = new BenchmarkSuite();

    suite
        .setScale(scale)
        .doSanitize(cmd.hasOption(OPT_SANITIZE))
        .add("getNid", () -> benchmarkGetNotificationId(bench, bData))
        .add("listDatabases", () -> benchmarkListDatabases(bench, bData))
        .add("listTables", () -> benchmarkListAllTables(bench, bData))
        .add("listTables" + '.' + instances,
            () -> benchmarkListTables(bench, bData, instances))
        .add("getTable", () -> benchmarkGetTable(bench, bData))
        .add("createTable", () -> benchmarkTableCreate(bench, bData))
        .add("dropTable", () -> benchmarkDeleteCreate(bench, bData))
        .add("dropTableWithPartitions",
            () -> benchmarkDeleteWithPartitions(bench, bData, 1, nparams))
        .add("dropTableWithPartitions" + '.' + instances,
            () -> benchmarkDeleteWithPartitions(bench, bData, instances, nparams))
        .add("addPartition", () -> benchmarkCreatePartition(bench, bData))
        .add("dropPartition", () -> benchmarkDropPartition(bench, bData))
        .add("listPartition", () -> benchmarkListPartition(bench, bData))
        .add("listPartitions" + '.' + instances,
            () -> benchmarkListManyPartitions(bench, bData,  instances))
        .add("getPartition",
            () -> benchmarkGetPartitions(bench, bData,  1))
        .add("getPartitions" + '.' + instances,
            () -> benchmarkGetPartitions(bench, bData,  instances))
        .add("getPartitionNames",
            () -> benchmarkGetPartitionNames(bench, bData,  1))
        .add("getPartitionNames" + '.' + instances,
            () -> benchmarkGetPartitionNames(bench, bData,  instances))
        .add("getPartitionsByNames",
            () -> benchmarkGetPartitionsByName(bench, bData,  1))
        .add("getPartitionsByNames" + '.' + instances,
            () -> benchmarkGetPartitionsByName(bench, bData,  instances))
        .add("addPartitions" + '.' + instances,
            () -> benchmarkCreatePartitions(bench, bData,  instances))
        .add("dropPartitions" + '.' + instances,
            () -> benchmarkDropPartitions(bench, bData,  instances))
        .add("renameTable",
            () -> benchmarkRenameTable(bench, bData,  1))
        .add("renameTable" + '.' + instances,
            () -> benchmarkRenameTable(bench, bData,  instances))
        .add("dropDatabase",
            () -> benchmarkDropDatabase(bench, bData, 1))
        .add("dropDatabase" + '.' + instances,
            () -> benchmarkDropDatabase(bench, bData, instances))
        .add("concurrentPartitionAdd" + "#" + nThreads,
            () -> benchmarkConcurrentPartitionOps(bench, bData,  instances, nThreads));

    if (doList) {
      suite.listMatching((patterns)).forEach(System.out::println);
      return;
    }

    LOG.info("Using table '{}.{}", dbName, tableName);

    try (HMSClient client =
             new HMSClient(getServerUri(cmd.getOptionValue(OPT_HOST), cmd.getOptionValue(OPT_PORT)),
                 cmd.getOptionValue(OPT_CONF))) {
      bData.setClient(client);
      if (!client.dbExists(dbName)) {
        client.createDatabase(dbName);
      }

      if (client.tableExists(dbName, tableName)) {
        client.dropTable(dbName, tableName);
      }

      // Arrange various benchmarks in a suite
      BenchmarkSuite result = suite.runMatching(patterns);

      if (cmd.hasOption(OPT_CSV)) {
        result.displayCSV(fmt, CSV_SEPARATOR);
      } else {
        result.display(fmt);
      }

      if (cmd.hasOption(OPT_OUTPUT)) {
        // Print results to stdout as well
        StringBuilder s = new StringBuilder();
        Formatter f = new Formatter(s);
        result.display(f);
        System.out.print(s);
        f.close();
      }

      output.print(sb.toString());
      fmt.close();

      if (cmd.hasOption(OPT_SAVEDATA)) {
        saveData(result.getResult(), cmd.getOptionValue(OPT_SAVEDATA), scale);
      }
    }
  }

  private static void saveData(Map<String,
      DescriptiveStatistics> result, String location, TimeUnit scale) throws IOException {
    Path dir = Paths.get(location);
    if (!Files.exists(dir)) {
      LOG.debug("creating directory {}", location);
      Files.createDirectories(dir);
    } else if (!Files.isDirectory(dir)) {
      LOG.error("{} should be a directory", location);
    }

    // Create a new file for each benchmark and dump raw data to it.
    result.forEach((name, data) -> saveDataFile(location, name, data, scale));
  }

  private static void saveDataFile(String location, String name,
                                   DescriptiveStatistics data, TimeUnit scale) {
    long conv = scale.toNanos(1);
    Path dst = Paths.get(location, name);
    try (PrintStream output = new PrintStream(dst.toString())) {
      // Print all values one per line
      Arrays.stream(data.getValues()).forEach(d -> output.println(d / conv));
    } catch (FileNotFoundException e) {
      LOG.error("failed to write to {}", dst.toString());
    }

  }

  private static void help(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("hbench [options] [name] ...", options);
    System.exit(0);
  }

}
