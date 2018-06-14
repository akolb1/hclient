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

import static com.akolb.HMSBenchmarks.benchmarkConcurrentPartitionOps;
import static com.akolb.HMSBenchmarks.benchmarkCreatePartition;
import static com.akolb.HMSBenchmarks.benchmarkCreatePartitions;
import static com.akolb.HMSBenchmarks.benchmarkDeleteCreate;
import static com.akolb.HMSBenchmarks.benchmarkDeleteWithPartitions;
import static com.akolb.HMSBenchmarks.benchmarkDropDatabase;
import static com.akolb.HMSBenchmarks.benchmarkDropPartition;
import static com.akolb.HMSBenchmarks.benchmarkDropPartitions;
import static com.akolb.HMSBenchmarks.benchmarkGetNotificationId;
import static com.akolb.HMSBenchmarks.benchmarkGetPartitionNames;
import static com.akolb.HMSBenchmarks.benchmarkGetPartitions;
import static com.akolb.HMSBenchmarks.benchmarkGetPartitionsByName;
import static com.akolb.HMSBenchmarks.benchmarkGetTable;
import static com.akolb.HMSBenchmarks.benchmarkListAllTables;
import static com.akolb.HMSBenchmarks.benchmarkListDatabases;
import static com.akolb.HMSBenchmarks.benchmarkListManyPartitions;
import static com.akolb.HMSBenchmarks.benchmarkListPartition;
import static com.akolb.HMSBenchmarks.benchmarkListTables;
import static com.akolb.HMSBenchmarks.benchmarkRenameTable;
import static com.akolb.HMSBenchmarks.benchmarkTableCreate;
import static com.akolb.HMSTool.OPT_CONF;
import static com.akolb.HMSTool.OPT_DATABASE;
import static com.akolb.HMSTool.OPT_HOST;
import static com.akolb.HMSTool.OPT_PORT;
import static com.akolb.HMSTool.OPT_NUMBER;
import static com.akolb.HMSTool.OPT_PARTITIONS;
import static com.akolb.HMSTool.OPT_PATTERN;
import static com.akolb.HMSTool.OPT_VERBOSE;
import static com.akolb.Util.getServerUri;

final class HMSBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(HMSBenchmark.class);
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

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("H", OPT_HOST, true, "HMS Server")
        .addOption("P", OPT_PORT, true, "HMS Server port")
        .addOption("p", OPT_PARTITIONS, true, "partitions list")
        .addOption("h", "help", false, "print this info")
        .addOption("d", OPT_DATABASE, true, "database name (can be regexp for list)")
        .addOption("v", OPT_VERBOSE, false, "verbose mode")
        .addOption("N", OPT_NUMBER, true, "number of instances")
        .addOption("K", OPT_SEPARATOR, true, "field separator")
        .addOption("L", OPT_SPIN, true, "spin count")
        .addOption("W", OPT_WARM, true, "warmup count")
        .addOption("l", OPT_LIST, true, "list benchmarks")
        .addOption("o", OPT_OUTPUT, true, "output file")
        .addOption("T", OPT_THREADS, true, "numberOfThreads")
        .addOption(new Option(OPT_CONF, true, "configuration directory"))
        .addOption(new Option(OPT_SANITIZE, false, "sanitize results"))
        .addOption(new Option(OPT_CSV, false, "produce CSV output"))
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

    PrintStream output = System.out;
    if (cmd.hasOption(OPT_OUTPUT)) {
      output = new PrintStream(cmd.getOptionValue(OPT_OUTPUT));
    }

    String dbName = cmd.getOptionValue(OPT_DATABASE);
    String tableName = TEST_TABLE;

    if (dbName == null || dbName.isEmpty()) {
      throw new RuntimeException("Missing DB name");
    }

    LOG.info("Using table '{}.{}", dbName, tableName);

    List<String> arguments = cmd.getArgList();

    boolean filtertests = cmd.hasOption(OPT_PATTERN);
    List<String> patterns = filtertests ?
        Lists.newArrayList(cmd.getOptionValue(OPT_PATTERN).split(",")) :
        Collections.emptyList();
    // If we have arguments, they are filters on the tests, so add them.
    if (!arguments.isEmpty()) {
      patterns = Stream.concat(patterns.stream(), arguments.stream()).collect(Collectors.toList());
    }

    try (HMSClient client =
             new HMSClient(getServerUri(cmd.getOptionValue(OPT_HOST), cmd.getOptionValue(OPT_PORT)),
                 cmd.getOptionValue(OPT_CONF))) {
      if (!client.dbExists(dbName)) {
        client.createDatabase(dbName);
      }

      if (client.tableExists(dbName, tableName)) {
        client.dropTable(dbName, tableName);
      }

      int instances = Integer.parseInt(cmd.getOptionValue(OPT_NUMBER, "100"));
      int nThreads =  Integer.parseInt(cmd.getOptionValue(OPT_THREADS, "2"));
      int warmup = Integer.parseInt(cmd.getOptionValue(OPT_WARM, "15"));
      int spin = Integer.parseInt(cmd.getOptionValue(OPT_SPIN, "100"));
      LOG.info("Using " + instances + " object instances" + " warmup " + warmup +
          " spin " + spin + " threads " + nThreads);

      final String db = dbName;
      final String tbl = tableName;

      LOG.info("Using {} object instances", instances);
      StringBuilder sb = new StringBuilder();
      Formatter fmt = new Formatter(sb);

      MicroBenchmark bench = new MicroBenchmark(warmup, spin);
      BenchmarkSuite suite = new BenchmarkSuite();

      // Arrange various benchmarks in a suite
      BenchmarkSuite result = suite
          .setScale(scale)
          .doSanitize(cmd.hasOption(OPT_SANITIZE))
          .add("getNid", () -> benchmarkGetNotificationId(bench, client))
          .add("listDatabases", () -> benchmarkListDatabases(bench, client))
          .add("listTables", () -> benchmarkListAllTables(bench, client, db))
          .add("listTables" + '.' + instances,
              () -> benchmarkListTables(bench, client, db, instances))
          .add("getTable", () -> benchmarkGetTable(bench, client, db, tbl))
          .add("createTable", () -> benchmarkTableCreate(bench, client, db, tbl))
          .add("dropTable", () -> benchmarkDeleteCreate(bench, client, db, tbl))
          .add("dropTableWithPartitions",
              () -> benchmarkDeleteWithPartitions(bench, client, db, tbl, 1))
          .add("dropTableWithPartitions" + '.' + instances,
              () -> benchmarkDeleteWithPartitions(bench, client, db, tbl, instances))
          .add("addPartition", () -> benchmarkCreatePartition(bench, client, db, tbl))
          .add("dropPartition", () -> benchmarkDropPartition(bench, client, db, tbl))
          .add("listPartition", () -> benchmarkListPartition(bench, client, db, tbl))
          .add("listPartitions" + '.' + instances,
              () -> benchmarkListManyPartitions(bench, client, db, tbl, instances))
          .add("getPartition",
              () -> benchmarkGetPartitions(bench, client, db, tbl, 1))
          .add("getPartitions" + '.' + instances,
              () -> benchmarkGetPartitions(bench, client, db, tbl, instances))
          .add("getPartitionNames",
              () -> benchmarkGetPartitionNames(bench, client, db, tbl, 1))
          .add("getPartitionNames" + '.' + instances,
              () -> benchmarkGetPartitionNames(bench, client, db, tbl, instances))
          .add("getPartitionsByNames",
              () -> benchmarkGetPartitionsByName(bench, client, db, tbl, 1))
          .add("getPartitionsByNames" + '.' + instances,
              () -> benchmarkGetPartitionsByName(bench, client, db, tbl, instances))
          .add("addPartitions" + '.' + instances,
              () -> benchmarkCreatePartitions(bench, client, db, tbl, instances))
          .add("dropPartitions" + '.' + instances,
              () -> benchmarkDropPartitions(bench, client, db, tbl, instances))
          .add("renameTable",
              () -> benchmarkRenameTable(bench, client, db, tbl, 1))
          .add("renameTable" + '.' + instances,
              () -> benchmarkRenameTable(bench, client, db, tbl, instances))
          .add("dropDatabase",
              () -> benchmarkDropDatabase(bench, client, db, 1))
          .add("dropDatabase" + '.' + instances,
              () -> benchmarkDropDatabase(bench, client, db, instances))
          .add("concurrentPartitionAdd" + "#" + nThreads,
              () -> benchmarkConcurrentPartitionOps(bench, client, db, tbl, instances, nThreads))
          .runMatching(patterns);

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
    formatter.printHelp("hbench ...", options);
    System.exit(0);
  }

}
