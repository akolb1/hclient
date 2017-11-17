package com.akolb;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hive.metastore.api.Table;

import java.time.temporal.ChronoUnit;
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

    try (HMSClient client = new HMSClient(server)) {

      if (!client.dbExists(dbName)) {
        client.createDatabase(dbName);
      }

      if (client.tableExists(dbName, tableName)) {
        client.dropTable(dbName, tableName);
      }

      benchmarkTableCreate(client, dbName, tableName);
    }
  }

  private static void benchmarkTableCreate(final HMSClient client,
                                           final String dbName, final String tableName) {
    Table table = makeTable(dbName, tableName, null, null);

    DescriptiveStatistics stats;
    DescriptiveStatistics delStats;

    MicroBenchmark bench = new MicroBenchmark();
    LOG.info("Measuring create table times");

    stats = bench.measure(null,
        () -> client.createTableNoException(table),
        () -> client.dropTableNoException(dbName, tableName));

    LOG.info("Measuring delete table times");
    delStats = bench.measure(
        () -> client.createTableNoException(table),
        () ->client.dropTableNoException(dbName, tableName),
        null);

    long scale = ChronoUnit.MILLIS.getDuration().getNano();

    double createError = stats.getStandardDeviation() / stats.getMean() * 100;
    double deleteError = delStats.getStandardDeviation() / delStats.getMean() * 100;

    System.out.printf("Create: Mean: %g ms, [%g, %g], +/- %g%% %n",
        stats.getMean() / scale, stats.getMin() / scale,
        stats.getMax() / scale,
        createError);

    System.out.printf("Delete: Mean: %g ms, [%g, %g], +/- %g%% %n",
        delStats.getMean() / scale, delStats.getMin() / scale,
        delStats.getMax() / scale,
        deleteError);
  }
}
