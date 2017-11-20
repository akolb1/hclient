package com.akolb;

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.Socket;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import static com.akolb.HMSClient.makeTable;
import static com.akolb.Main.DEFAULT_PORT;
import static com.akolb.Main.OPT_DATABASE;
import static com.akolb.Main.OPT_DROP;
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
  private static Logger LOG = Logger.getLogger(HMSBenchmark.class.getName());
  private static long scale = ChronoUnit.MILLIS.getDuration().getNano();

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("s", OPT_SERVER, true, "HMS Server")
        .addOption("P", OPT_PARTITIONS, true, "partitions list")
        .addOption("h", "help", false, "print this info")
        .addOption("d", OPT_DATABASE, true, "database name (can be regexp for list)")
        .addOption("t", OPT_TABLE, true, "table name (can be regexp for list)")
        .addOption("v", OPT_VERBOSE, false, "verbose mode")
        .addOption("N", OPT_NUMBER, true, "number of instances")
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

    LOG.info("connecting to " + server);

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

    LOG.info("Using table '" + dbName + "." + tableName + "'");

    boolean filtertests = cmd.hasOption(OPT_PATTERN);
    List<String> patterns = filtertests ?
        Lists.newArrayList(cmd.getOptionValue(OPT_PATTERN).split(",")):
        Collections.emptyList();

    try (HMSClient client = new HMSClient(server)) {

      if (!client.dbExists(dbName)) {
        client.createDatabase(dbName);
      }

      if (client.tableExists(dbName, tableName)) {
        client.dropTable(dbName, tableName);
      }

      MicroBenchmark bench = new MicroBenchmark();
      BenchmarkSuite suite = new BenchmarkSuite();
      final String hostName =  getServerUri(cmd).getHost();
      final String db = dbName;
      final String tbl = tableName;

      suite.add("latency",       () -> benchmarkNetworkLatency(hostName, DEFAULT_PORT));
      suite.add("listDatabases", () -> bench.measure(client::getAllDatabasesNoException));
      suite.add("listTables",    () -> bench.measure(() -> client.getAllTablesNoException(db)));
      suite.add("listTables100", () -> benchmarkListTables(bench, client, db, 100));
      suite.add("getTable",      () -> benchmarkGetTable(bench, client, db, tbl));
      suite.add("createTable",   () -> benchmarkTableCreate(bench, client, db, tbl));
      suite.add("deleteTable",   () -> benchmarkDeleteCreate(bench, client, db, tbl));

      // Run all tests and disolay results
      System.out.printf("%-20s %-6s %-6s %-6s %-6s%n",
          "Operation", "Mean", "Min", "Max", "Err%");
        suite.runMatching(patterns).forEach(HMSBenchmark::displayStats);
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

  private static DescriptiveStatistics benchmarkNetworkLatency(final String server, int port) {
    MicroBenchmark bench = new MicroBenchmark(10, 50);
    return bench.measure(
        () -> {
          try (Socket socket = new Socket(server, port)) {
          } catch (IOException e) {
            e.printStackTrace();
          }
        });
  }

  private static DescriptiveStatistics benchmarkGetTable(MicroBenchmark bench,
                                        final HMSClient client, final String dbName,
                                        final String tableName) {
    List<FieldSchema> columns = createSchema(new ArrayList<>(Arrays.asList("name", "string")));
    List<FieldSchema> partitions = createSchema(new ArrayList<>(Arrays.asList("date", "string")));

    Table table = makeTable(dbName, tableName, columns, partitions);
    client.createTableNoException(table);
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
      try {
        dropManyTables(client, count, dbName, format);
      } catch (TException e) {
        e.printStackTrace();
      }
    }
  }

  private static void createManyTables(HMSClient client, int howMany, String dbName, String format)
      throws TException {
    List<FieldSchema> columns = createSchema(new ArrayList<>(Arrays.asList("name", "string")));
    List<FieldSchema> partitions = createSchema(new ArrayList<>(Arrays.asList("date", "string")));
    for (int i = 0; i < howMany; i++) {
      String tName = String.format(format, i);
      Table table = makeTable(dbName, tName, columns, partitions);
      client.createTable(table);
    }
  }

  private static void dropManyTables(HMSClient client, int howMany, String dbName, String format)
      throws TException {
    for (int i = 0; i < howMany; i++) {
      String tName = String.format(format, i);
      client.dropTable(dbName, tName);
    }
  }

  private static void displayStats(String name, DescriptiveStatistics stats) {
    double err = stats.getStandardDeviation() / stats.getMean() * 100;
    System.out.printf("%-20s %-6.3g %-6.3g %-6.3g %-6.3g%n", name,
        stats.getMean() / scale,
        stats.getMin() / scale,
        stats.getMax() / scale,
        err);
  }
}
