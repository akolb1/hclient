package com.akolb;

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.PrintStream;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;

import static com.akolb.HMSBenchmarks.benchmarkCreatePartition;
import static com.akolb.HMSBenchmarks.benchmarkDeleteCreate;
import static com.akolb.HMSBenchmarks.benchmarkDropPartition;
import static com.akolb.HMSBenchmarks.benchmarkGetNotificationId;
import static com.akolb.HMSBenchmarks.benchmarkGetTable;
import static com.akolb.HMSBenchmarks.benchmarkListAllTables;
import static com.akolb.HMSBenchmarks.benchmarkListDatabases;
import static com.akolb.HMSBenchmarks.benchmarkListManyPartitions;
import static com.akolb.HMSBenchmarks.benchmarkListPartition;
import static com.akolb.HMSBenchmarks.benchmarkListTables;
import static com.akolb.HMSBenchmarks.benchmarkTableCreate;
import static com.akolb.Main.OPT_CONF;
import static com.akolb.Main.OPT_DATABASE;
import static com.akolb.Main.OPT_HOST;
import static com.akolb.Main.OPT_NUMBER;
import static com.akolb.Main.OPT_PARTITIONS;
import static com.akolb.Main.OPT_PATTERN;
import static com.akolb.Main.OPT_TABLE;
import static com.akolb.Main.OPT_VERBOSE;
import static com.akolb.Main.getServerUri;

    /*
     * TODO support saving raw data to files
     * TODO support CSV output
     * TODO support saving results to file
     */

class HMSBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(HMSBenchmark.class);
  private static final long scale = ChronoUnit.MILLIS.getDuration().getNano();
  private static final String CSV_SEPARATOR = "\t";

  private static final String OPT_SEPARATOR = "separator";
  private static final String OPT_SPIN = "spin";
  private static final String OPT_WARM = "warm";
  private static final String OPT_LIST = "list";
  private static final String OPT_SANITIZE = "sanitize";
  private static final String OPT_OUTPUT = "output";
  private static final String OPT_CSV = "csv";

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("H", OPT_HOST, true, "HMS Server")
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
        .addOption("o", OPT_OUTPUT, true, "output file")
        .addOption(new Option(OPT_CONF, true, "configuration directory"))
        .addOption(new Option(OPT_SANITIZE, false, "sanitize results"))
        .addOption(new Option(OPT_CSV, false, "produce CSV output"))
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

    PrintStream output = System.out;
    if (cmd.hasOption(OPT_OUTPUT)) {
      output = new PrintStream(cmd.getOptionValue(OPT_OUTPUT));
    }

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

    try (HMSClient client =
             new HMSClient(getServerUri(cmd.getOptionValue(OPT_HOST)),
                 cmd.getOptionValue(OPT_CONF))) {
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

      final String db = dbName;
      final String tbl = tableName;

      LOG.info("Using {} object instances", instances);
      StringBuilder sb = new StringBuilder();
      Formatter fmt = new Formatter(sb);

      MicroBenchmark bench = new MicroBenchmark(warmup, spin);
      BenchmarkSuite suite = new BenchmarkSuite(cmd.hasOption(OPT_SANITIZE));

      BenchmarkSuite result = suite
          .setScale(scale)
          .setFmt(fmt)
          .add("listDatabases", () -> benchmarkListDatabases(bench, client))
          .add("listTables", () -> benchmarkListAllTables(bench, client, db))
          .add("listTablesN", () -> benchmarkListTables(bench, client, db, instances))
          .add("getTable", () -> benchmarkGetTable(bench, client, db, tbl))
          .add("createTable", () -> benchmarkTableCreate(bench, client, db, tbl))
          .add("dropTable", () -> benchmarkDeleteCreate(bench, client, db, tbl))
          .add("addPartition", () -> benchmarkCreatePartition(bench, client, db, tbl))
          .add("dropPartition", () -> benchmarkDropPartition(bench, client, db, tbl))
          .add("listPartition", () -> benchmarkListPartition(bench, client, db, tbl))
          .add("listPartitions", () -> benchmarkListManyPartitions(bench, client, db, tbl,
              instances))
          .add("getNid", () -> benchmarkGetNotificationId(bench, client))
          .runMatching(patterns);

      if (cmd.hasOption(OPT_CSV)) {
        result.displayCSV(CSV_SEPARATOR);
      } else {
        result.display();
      }

      output.print(sb.toString());
    }
  }

  private static void help(Options options) {
    HelpFormatter formater = new HelpFormatter();
    formater.printHelp("hbench ...", options);
    System.exit(0);
  }

}
