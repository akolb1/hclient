package com.akolb;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import static com.akolb.HMSClient.makeTable;
import static com.akolb.Main.OPT_DATABASE;
import static com.akolb.Main.OPT_DROP;
import static com.akolb.Main.OPT_NUMBER;
import static com.akolb.Main.OPT_PARTITIONS;
import static com.akolb.Main.OPT_PATTERN;
import static com.akolb.Main.OPT_PORT;
import static com.akolb.Main.OPT_SERVER;
import static com.akolb.Main.OPT_TABLE;
import static com.akolb.Main.OPT_VERBOSE;
import static com.akolb.Main.createSchema;
import static com.akolb.Main.getServerUri;
import static com.akolb.Main.help;

public class HMSBenchmark {
  private static Logger LOG = Logger.getLogger(HMSBenchmark.class.getName());

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("s", OPT_SERVER, true, "HMS Server")
        .addOption("p", OPT_PORT, true, "port")
        .addOption("P", OPT_PARTITIONS, true, "partitions list")
        .addOption("h", "help", false, "print this info")
        .addOption("d", OPT_DATABASE, true, "database name (can be regexp for list)")
        .addOption("t", OPT_TABLE, true, "table name (can be regexp for list)")
        .addOption("v", OPT_VERBOSE, false, "verbose mode")
        .addOption("N", OPT_NUMBER, true, "number of instances")
        .addOption("S", OPT_PATTERN, true, "table name pattern for bulk creation")
        .addOption("D", OPT_DROP, false, "drop table if exists");

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

    String server = getServerUri(cmd);

    LOG.info("connecting to " + server);

    HMSClient client = new HMSClient(server);
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

    if (!client.dbExists(dbName)) {
      client.createDatabase(dbName);
    }

    if (client.tableExists(dbName, tableName)) {
      client.dropTable(dbName, tableName);
    }

    benchmarkTableCreate(client, dbName, tableName);
  }

  private static void benchmarkTableCreate(HMSClient client, String dbName, String tableName)
      throws TException {
    List<FieldSchema> tableSchema = createSchema(Collections.emptyList());
    List<FieldSchema> partitionSchema = createSchema(Collections.emptyList());
    Table table = makeTable(dbName, tableName, tableSchema, partitionSchema);

    DescriptiveStatistics stats = new DescriptiveStatistics();
    DescriptiveStatistics delStats = new DescriptiveStatistics();

    System.out.println("Warmup");
    for (int i = 0; i < 20; i++) {
      client.createTable(table);
      client.dropTable(dbName, tableName);
    }

    System.out.println("Starting benchmark");
    for (int i = 0; i < 100; i++) {
      long begin = System.nanoTime();
      client.createTable(table);
      long end = System.nanoTime();
      stats.addValue((double)(end - begin));
      begin = System.nanoTime();
      client.dropTable(dbName, tableName);
      end = System.nanoTime();
      delStats.addValue((double)(end - begin));
    }
    System.out.println("Finished benchmark");

    System.out.printf("Create: Mean: %g, [%g, %g], +/- %g\n",
        stats.getMean() / 1000000, stats.getMin() / 1000000,
        stats.getMax() / 1000000,
        stats.getStandardDeviation() / 1000000);
    System.out.printf("Delete: Mean: %g, [%g, %g], +/- %g\n",
        delStats.getMean() / 1000000, delStats.getMin() / 1000000,
        delStats.getMax() / 1000000,
        delStats.getStandardDeviation() / 1000000);
  }
}
