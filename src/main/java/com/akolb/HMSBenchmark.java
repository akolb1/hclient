package com.akolb;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.net.Socket;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.akolb.HMSClient.makeTable;
import static com.akolb.Main.DEFAULT_PORT;
import static com.akolb.Main.OPT_DATABASE;
import static com.akolb.Main.OPT_NUMBER;
import static com.akolb.Main.OPT_PARTITIONS;
import static com.akolb.Main.OPT_PATTERN;
import static com.akolb.Main.OPT_SERVER;
import static com.akolb.Main.OPT_TABLE;
import static com.akolb.Main.OPT_VERBOSE;
import static com.akolb.Main.createSchema;
import static com.akolb.Main.getServerUri;
import static com.akolb.Main.help;

public class HMSBenchmark {
  private static Logger LOG = LoggerFactory.getLogger(HMSBenchmark.class.getName());
  private static long scale = ChronoUnit.MILLIS.getDuration().getNano();

  private static final String OPT_SEPARATOR = "separator";
  private static final String OPT_SPIN = "spin";
  private static final String OPT_WARM = "warm";
  private static final String OPT_LIST = "list";

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("s", OPT_SERVER, true, "HMS Server")
        .addOption("P", OPT_PARTITIONS, true, "partitions list")
        .addOption("h", "help", false, "print this info")
        .addOption("d", OPT_DATABASE, true, "database name (can be regexp for list)")
        .addOption("t", OPT_TABLE, true, "table name (can be regexp for list)")
        .addOption("v", OPT_VERBOSE, false, "verbose mode")
        .addOption("N", OPT_NUMBER, true, "number of instances")
        .addOption("K", OPT_SEPARATOR, true, "field separator")
        .addOption("L", OPT_SPIN, true, "spin count")
        .addOption("W", OPT_WARM, true, "warmup count")
        .addOption("l", OPT_LIST, true, "list benchmarks")
        .addOption("S", OPT_PATTERN, true, "test patterns");

    CommandLineParser parser = new DefaultParser();

    CommandLine cmd = null;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      help(options);
      System.exit(1);
    }

    if (cmd.hasOption("help")) {
      help(options);
    }

    String server = getServerUri(cmd).toString();

    LOG.info("connecting to {}", server);

    String dbName = cmd.getOptionValue(OPT_DATABASE);
    String tableName = cmd.getOptionValue(OPT_TABLE);

    if (tableName != null && tableName.contains(".")) {
      String[] parts = tableName.split("\\.");
      dbName = parts[0];
      tableName = parts[1];
    }

    if (dbName == null || dbName.isEmpty()) {
      throw new RuntimeException("Missing DB name");
    }
    if (tableName == null || tableName.isEmpty()) {
      throw new RuntimeException("Missing Table name");
    }

    LOG.info("Using table '{}.{}", dbName, tableName);

    boolean filtertests = cmd.hasOption(OPT_PATTERN);
    List<String> patterns = filtertests ?
        Lists.newArrayList(cmd.getOptionValue(OPT_PATTERN).split(",")) :
        Collections.emptyList();

    try (HMSClient client = new HMSClient(server)) {

      if (!client.dbExists(dbName)) {
        client.createDatabase(dbName);
      }

      if (client.tableExists(dbName, tableName)) {
        client.dropTable(dbName, tableName);
      }

      int instances = Integer.parseInt(cmd.getOptionValue(OPT_NUMBER, "100"));
      int warmup = Integer.parseInt(cmd.getOptionValue(OPT_WARM, "15"));
      int spin = Integer.parseInt(cmd.getOptionValue(OPT_SPIN, "100"));
      LOG.info("Using " + instances + " object instances" + " warmup " + warmup +
          " spin " + spin);

      MicroBenchmark bench = new MicroBenchmark(warmup, spin);
      BenchmarkSuite suite = new BenchmarkSuite();
      final String hostName = getServerUri(cmd).getHost();
      final String db = dbName;
      final String tbl = tableName;

      LOG.info("Using {} object instances", instances);

      displayStats(suite
          .add("0-latency-0",   () -> benchmarkNetworkLatency(bench, hostName, DEFAULT_PORT))
          .add("listDatabases", () -> bench.measure(client::getAllDatabasesNoException))
          .add("listTables",    () -> bench.measure(() -> client.getAllTablesNoException(db)))
          .add("listTablesN",   () -> benchmarkListTables(bench, client, db, instances))
          .add("getTable",      () -> benchmarkGetTable(bench, client, db, tbl))
          .add("createTable",   () -> benchmarkTableCreate(bench, client, db, tbl))
          .add("dropTable",     () -> benchmarkDeleteCreate(bench, client, db, tbl))
          .add("addPartition",  () -> benchmarkCreatePartition(bench, client, db, tbl))
          .add("dropPartition", () -> benchmarkDropPartition(bench, client, db, tbl))
          .add("listPartition", () -> benchmarkListPartition(bench, client, db, tbl))
          .add("listPartitions",() -> benchmarkListManyPartitions(bench, client, db, tbl, instances))
          .runMatching(patterns), cmd.getOptionValue(OPT_SEPARATOR));
    }
  }

  private static DescriptiveStatistics benchmarkTableCreate(MicroBenchmark bench,
                                                            final HMSClient client,
                                                            final String dbName,
                                                            final String tableName) {
    Table table = makeTable(dbName, tableName, null, null);

    return bench.measure(null,
        () -> client.createTableNoException(table),
        () -> client.dropTableNoException(dbName, tableName));
  }

  private static DescriptiveStatistics benchmarkDeleteCreate(MicroBenchmark bench,
                                                             final HMSClient client,
                                                             final String dbName,
                                                             final String tableName) {
    Table table = makeTable(dbName, tableName, null, null);

    return bench.measure(
        () -> client.createTableNoException(table),
        () -> client.dropTableNoException(dbName, tableName),
        null);
  }

  private static DescriptiveStatistics benchmarkNetworkLatency(MicroBenchmark bench,
                                                               final String server, int port) {
    return bench.measure(
        () -> {
          try (Socket socket = new Socket(server, port)) {
          } catch (IOException e) {
            e.printStackTrace();
          }
        });
  }

  private static DescriptiveStatistics benchmarkGetTable(MicroBenchmark bench,
                                                         final HMSClient client,
                                                         final String dbName,
                                                         final String tableName) {
    createPartitionedTable(client, dbName, tableName);
    try {
      return bench.measure(() -> client.getTableNoException(dbName, tableName));
    } finally {
      client.dropTableNoException(dbName, tableName);
    }
  }

  private static DescriptiveStatistics benchmarkListTables(MicroBenchmark bench,
                                                           HMSClient client,
                                                           String dbName,
                                                           int count) {
    // Create a bunch of tables
    String format = "tmp_table_%d";
    try {
      createManyTables(client, count, dbName, format);
      return bench.measure(() -> client.getAllTablesNoException(dbName));
    } catch (TException e) {
      e.printStackTrace();
      return new DescriptiveStatistics();
    } finally {
        dropManyTables(client, count, dbName, format);
    }
  }

  private static DescriptiveStatistics benchmarkCreatePartition(MicroBenchmark bench,
                                                                final HMSClient client,
                                                                final String dbName,
                                                                final String tableName) {
    createPartitionedTable(client, dbName, tableName);
    final List<String> values = Collections.singletonList("d1");
    try {
      Table t = client.getTable(dbName, tableName);
      Partition partition = HMSClient.makePartition(t, values);
      return bench.measure(null,
          () -> client.createPartitionNoException(partition),
          () -> client.dropPartitionNoException(dbName, tableName, values));
    } catch (TException e) {
      e.printStackTrace();
      return new DescriptiveStatistics();
    } finally {
      client.dropTableNoException(dbName, tableName);
    }
  }

  private static DescriptiveStatistics benchmarkListPartition(MicroBenchmark bench,
                                                              final HMSClient client,
                                                              final String dbName,
                                                              final String tableName) {
    createPartitionedTable(client, dbName, tableName);
    try {
      client.addManyPartitions(dbName, tableName,
          Collections.singletonList("d"), 1);

      return bench.measure(() -> client.listPartitionsNoException(dbName, tableName));
    } catch (TException e) {
      e.printStackTrace();
      return new DescriptiveStatistics();
    } finally {
      client.dropTableNoException(dbName, tableName);
    }
  }

  private static DescriptiveStatistics benchmarkListManyPartitions(MicroBenchmark bench,
                                                                   final HMSClient client,
                                                                   final String dbName,
                                                                   final String tableName,
                                                                   int howMany) {
    createPartitionedTable(client, dbName, tableName);
    try {
      client.addManyPartitions(dbName, tableName,
          Collections.singletonList("d"), howMany);
      LOG.info("Created {} partitions", howMany);
      LOG.info("starting benchmark... ");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      LOG.info("started benchmark... ");
      return bench.measure(() -> client.listPartitionsNoException(dbName, tableName));
    } catch (TException e) {
      e.printStackTrace();
      return new DescriptiveStatistics();
    } finally {
      client.dropTableNoException(dbName, tableName);
    }
  }

  private static DescriptiveStatistics benchmarkDropPartition(MicroBenchmark bench,
                                                                final HMSClient client,
                                                                final String dbName,
                                                                final String tableName) {
    createPartitionedTable(client, dbName, tableName);
    final List<String> values = Collections.singletonList("d1");
    try {
      Table t = client.getTable(dbName, tableName);
      Partition partition = HMSClient.makePartition(t, values);
      return bench.measure(() -> client.createPartitionNoException(partition),
          () -> client.dropPartitionNoException(dbName, tableName, values),
          null);
    } catch (TException e) {
      e.printStackTrace();
      return new DescriptiveStatistics();
    } finally {
      client.dropTableNoException(dbName, tableName);
    }
  }

  private static void createManyTables(HMSClient client, int howMany, String dbName, String format)
      throws TException {
    List<FieldSchema> columns = createSchema(new ArrayList<>(Arrays.asList("name", "string")));
    List<FieldSchema> partitions = createSchema(new ArrayList<>(Arrays.asList("date", "string")));
    IntStream.range(0, howMany)
        .forEach(i ->
            client.createTableNoException(makeTable(dbName,
            String.format(format, i), columns, partitions)));
  }

  private static void dropManyTables(HMSClient client, int howMany, String dbName, String format) {
    IntStream.range(0, howMany)
        .forEach(i ->
            client.dropTableNoException(dbName, String.format(format, i)));
  }

  // Create a simple table with a single column and single partition
  private static void createPartitionedTable(HMSClient client, String dbName, String tableName) {
    client.createTableNoException(makeTable(dbName, tableName,
        createSchema(Collections.singletonList("name:string")),
        createSchema(Collections.singletonList("date"))));
  }

  private static void displayStats(Map<String, DescriptiveStatistics> stats,
                                   @Nullable String separator) {
    if (separator != null && !separator.isEmpty()) {
      System.out.println(Joiner.on(separator).join(new ArrayList<>(Arrays.asList(
          "Operation",
          "Mean",
          "Max",
          "Err%"))));
      stats.forEach((name, value) -> displaySeparatedStats(name, value, separator));
    } else {
      System.out.printf("%-20s %-6s %-6s %-6s %-6s%n",
          "Operation", "Mean", "Min", "Max", "Err%");
      stats.forEach(HMSBenchmark::displayStats);
    }
  }


  private static void displayStats(String name, DescriptiveStatistics stats) {
    double err = stats.getStandardDeviation() / stats.getMean() * 100;
    System.out.printf("%-20s %-6.3g %-6.3g %-6.3g %-6.3g%n",
        name,
        stats.getMean() / scale,
        stats.getMin() / scale,
        stats.getMax() / scale,
        err);
  }

  private static void displaySeparatedStats(String name, DescriptiveStatistics stats,
                                   @Nonnull String separator) {
    double err = stats.getStandardDeviation() / stats.getMean() * 100;
    System.out.println(Joiner.on(separator).join(new ArrayList<>(Arrays.asList(
            name,
            String.format("%g", stats.getMean()),
            String.format("%g", stats.getMin()),
            String.format("%g", stats.getMax()),
            String.format("%g", err)))));
  }

}
