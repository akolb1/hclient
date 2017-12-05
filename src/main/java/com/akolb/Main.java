package com.akolb;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);
  // Default column type
  private static final String DEFAULT_TYPE = "string";
  private static final String TYPE_SEPARATOR = ":";

  private static final String DEFAULT_HOST = "localhost";
  private static final String THRIFT_SCHEMA = "thrift";
  static final int DEFAULT_PORT = 9083;

  private static final String DBNAME = "default";

  static final String OPT_HOST = "server";
  static final String OPT_PARTITIONS = "partitions";
  static final String OPT_DATABASE = "database";
  static final String OPT_TABLE = "table";
  static final String OPT_DROP = "drop";
  static final String OPT_VERBOSE = "verbose";
  static final String OPT_NUMBER = "number";
  static final String OPT_PATTERN = "pattern";

  private static final String DEFAULT_PATTERN = "%s_%d";
  static final String ENV_SERVER = "HMS_THRIFT_SERVER";

  private static final String CMD_LIST = "list";
  private static final String CMD_CREATE = "create";
  private static final String CMD_ADD_PART = "addpart";
  private static final String CMD_DROP = "drop";
  private static final String CMD_LIST_NID = "currnid";


  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("H", OPT_HOST, true, "HMS Server")
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

    List<String> arguments = cmd.getArgList();
    String command = CMD_LIST;
    if (!arguments.isEmpty()) {
      command = arguments.get(0);
      arguments = arguments.subList(1, arguments.size());
    }

    String dbName = cmd.getOptionValue(OPT_DATABASE);
    String tableName = cmd.getOptionValue(OPT_TABLE);

    String partitionsInfo = cmd.getOptionValue(OPT_PARTITIONS);
    String[] partitions = partitionsInfo == null ? null : partitionsInfo.split(",");
    List<String> partitionInfo = partitions == null ?
        Collections.emptyList() :
        new ArrayList<>(Arrays.asList(partitions));
    boolean verbose = cmd.hasOption(OPT_VERBOSE);

    try (HMSClient client = new HMSClient(getServerUri(cmd.getOptionValue(OPT_HOST)))) {
      switch (command) {
        case CMD_LIST:
          displayTables(client, dbName, tableName, verbose);
          break;

        case CMD_CREATE:
          if (tableName != null && tableName.contains(".")) {
            String[] parts = tableName.split("\\.");
            dbName = parts[0];
            tableName = parts[1];
          }

          boolean multiple = false;
          int nTables = 0;
          if (cmd.hasOption(OPT_NUMBER)) {
            nTables = Integer.valueOf(cmd.getOptionValue(OPT_NUMBER, "0"));
            if (nTables > 0) {
              multiple = true;
            }
          }

          if (dbName == null) {
            dbName = DBNAME;
          }

          if (tableName == null) {
            LOG.warn("Missing table name");
            System.exit(1);
          }

          if (!client.dbExists(dbName)) {
            client.createDatabase(dbName);
          } else {
            LOG.warn("Database '" + dbName + "' already exist");
          }

          if (!multiple) {
            if (client.tableExists(dbName, tableName)) {
              if (cmd.hasOption(OPT_DROP)) {
                LOG.warn("Dropping existing table '" + tableName + "'");
                client.dropTable(dbName, tableName);
              } else {
                LOG.warn("Table '" + tableName + "' already exist");
                break;
              }
            }

            client.createTable(HMSClient.makeTable(dbName, tableName,
                createSchema(arguments),
                createSchema(partitionInfo)));
            LOG.info("Created table '" + tableName + "'");
          } else {
            Set<String> tables = client.getAllTables(dbName, null);
            for (int i = 1; i <= nTables; i++) {
              String pattern = cmd.getOptionValue(OPT_PATTERN, DEFAULT_PATTERN);
              String tbl = String.format(pattern, tableName, i);
              if (tables.contains(tbl)) {
                if (cmd.hasOption(OPT_DROP)) {
                  LOG.warn("Dropping existing table '" + tbl + "'");
                  client.dropTable(dbName, tbl);
                } else {
                  LOG.warn("Table '" + tbl + "' already exist");
                  break;
                }
              }

              client.createTable(HMSClient.makeTable(dbName, tbl,
                  createSchema(arguments),
                  createSchema(partitionInfo)));
              tables.add(tbl);
            }
          }
          break;
        case CMD_ADD_PART:
          if (tableName != null && tableName.contains(".")) {
            String[] parts = tableName.split("\\.");
            dbName = parts[0];
            tableName = parts[1];
          }
          if (cmd.hasOption(OPT_NUMBER)) {
            int nPartitions = Integer.parseInt(cmd.getOptionValue(OPT_NUMBER));
            client.addManyPartitions(dbName, tableName, arguments, nPartitions);
          } else {
            addPartition(client, dbName, tableName, arguments);
          }
          break;

        case CMD_DROP:
          if (dbName == null || dbName.isEmpty()) {
            System.out.println("Missing database name");
            System.exit(1);
          }
          if (!arguments.isEmpty()) {
            // Drop partition case
            client.dropPartition(dbName, tableName, arguments);
          } else {
            dropTables(client, dbName, tableName);
          }
          break;

        case CMD_LIST_NID:
          System.out.println(client.getCurrentNotificationId());
          break;

        default:
          LOG.warn("Unknown command '" + command + "'");
          System.exit(1);
      }
    }
  }

  static void help(Options options) {
    HelpFormatter formater = new HelpFormatter();
    formater.printHelp("hclient list|create|addpart <options> [name:type...]", options);
    System.exit(0);
  }

  static @Nullable URI getServerUri(@Nullable String host) {
    if (host == null) {
      return null;
    }

    try {
      return new URI(THRIFT_SCHEMA, null, host, DEFAULT_PORT,
          null, null, null);
    } catch (URISyntaxException e) {
      e.printStackTrace();
      System.exit(1);
    }
    return null;
  }

  /**
   * Create table schema from parameters
   *
   * @param params list of parameters. Each parameter can be either a simple name or
   *               name:type for non-String types.
   * @return table schema description
   */
  static List<FieldSchema> createSchema(List<String> params) {
    if (params == null || params.isEmpty()) {
      return Collections.emptyList();
    }

    ArrayList<FieldSchema> cols = new ArrayList<>(params.size());
    for (String param : params) {
      String colType = DEFAULT_TYPE;
      String name = param;
      if (param.contains(TYPE_SEPARATOR)) {
        String[] parts = param.split(TYPE_SEPARATOR);
        name = parts[0];
        colType = parts[1].toLowerCase();
      }
      cols.add(new FieldSchema(name, colType, ""));
    }
    return cols;
  }

  private static void displayTables(HMSClient client, String dbName,
                                    String tableName, boolean verbose) throws MetaException {
    for (String database : client.getAllDatabases(dbName)) {
      client.getAllTables(database, tableName)
          .stream()
          .sorted()
          .forEach(tblName -> {
            if (verbose) {
              Table table = client.getTableNoException(database, tblName);
              displayTableSchema(table);
              String tableLocation = table.getSd().getLocation() + "/";
              client.listPartitionsNoException(database, tblName)
                  .forEach(p -> System.out.println("\t\t" +
                      p
                          .getSd()
                          .getLocation()
                          .replaceAll(tableLocation, "")));
              System.out.println();
            } else {
              System.out.println(database + "." + tblName);
            }
          });
    }
  }

  private static void dropTables(HMSClient client, String dbName, String tableName)
      throws MetaException {
    for (String database : client.getAllDatabases(dbName)) {
      client.getAllTables(database, tableName)
          .stream()
          .sorted()
          .forEach(tblName -> client.dropTableNoException(dbName, tblName));
    }
  }

  private static void addPartition(HMSClient client, String dbName, String tableName,
                                   List<String> values) throws TException {
    if (dbName == null || dbName.isEmpty()) {
      System.out.println("Missing database name");
      System.exit(1);
    }
    if (tableName == null || tableName.isEmpty()) {
      System.out.println("Missing Table name");
      System.exit(1);
    }

    Table table = client.getTable(dbName, tableName);
    client.createPartition(table, values);
  }

  private static void displayTableSchema(Table table) {
    String dbName = table.getDbName();
    String tableName = table.getTableName();
    System.out.println(dbName + "." + tableName);
    table.getSd().getCols()
        .forEach(schema -> System.out.println("\t" + schema.getName() + ":\t" + schema.getType()));
    table.getPartitionKeys()
        .forEach(schema -> System.out.println("\t  " + schema.getName() + ":\t" + schema.getType()));
  }
}
