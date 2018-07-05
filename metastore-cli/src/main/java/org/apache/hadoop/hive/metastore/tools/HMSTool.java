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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.hadoop.hive.metastore.tools.Constants.OPT_CONF;
import static org.apache.hadoop.hive.metastore.tools.Constants.OPT_DATABASE;
import static org.apache.hadoop.hive.metastore.tools.Constants.OPT_PORT;
import static org.apache.hadoop.hive.metastore.tools.Constants.OPT_VERBOSE;
import static org.apache.hadoop.hive.metastore.tools.Util.throwingSupplierWrapper;

// TODO Handle HADOOP_CONF_DIR and HADOOP_HOME

/**
 * Command-line Hive metastore tool.
 */
final class HMSTool {
  private static final Logger LOG = LoggerFactory.getLogger(HMSTool.class);

  private static final String DBNAME = "default";

  private static final String OPT_HOST = "host";
  private static final String OPT_PARTITIONS = "partitions";
  private static final String OPT_COLUMNS = "columns";
  private static final String OPT_TABLE = "table";
  private static final String OPT_DROP = "drop";
  private static final String OPT_NUMBER = "number";
  private static final String OPT_PATTERN = "pattern";
  private static final String OPT_SHOW_PARTS = "showparts";

  private static final String DEFAULT_PATTERN = "%s_%d";

  private static final String CMD_LIST = "list";
  private static final String CMD_CREATE = "create";
  private static final String CMD_ADD_PART = "addpart";
  private static final String CMD_DROP = "drop";
  private static final String CMD_LIST_NID = "currnid";
  private static final String CMD_RENAME = "rename";
  private static final String CMD_DROPDB = "dropdb";


  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("H", OPT_HOST, true, "HMS Server")
        .addOption("P", OPT_PORT, true, "HMS Server port")
        .addOption("p", OPT_PARTITIONS, true, "partitions list")
        .addOption("c", OPT_COLUMNS, true, "column schema")
        .addOption("h", "help", false, "print this info")
        .addOption("d", OPT_DATABASE, true, "database name (can be regexp for list)")
        .addOption("t", OPT_TABLE, true, "table name (can be regexp for list)")
        .addOption("v", OPT_VERBOSE, false, "verbose mode")
        .addOption("N", OPT_NUMBER, true, "number of instances")
        .addOption("S", OPT_PATTERN, true, "table name pattern for bulk creation")
        .addOption(new Option(OPT_CONF, true, "configuration directory"))
        .addOption(new Option(OPT_SHOW_PARTS, false, "show partitions"))
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

    LogUtils.initHiveLog4j();

    try (HMSClient client =
             new HMSClient(Util.getServerUri(cmd.getOptionValue(OPT_HOST), cmd.getOptionValue(OPT_PORT)),
                 cmd.getOptionValue(OPT_CONF))) {
      switch (command) {
        case CMD_LIST:
          cmdDisplayTables(client, cmd);
          break;
        case CMD_CREATE:
          cmdCreate(client, cmd, arguments);
          break;
        case CMD_ADD_PART:
          cmdAddPart(client, cmd, arguments);
          break;
        case CMD_DROP:
          cmdDrop(client, cmd, arguments);
          break;
        case CMD_RENAME:
          cmdRename(client, cmd, arguments);
          break;
        case CMD_LIST_NID:
          System.out.println(client.getCurrentNotificationId());
          break;
        case CMD_DROPDB:
          cmdDropDatabase(client, cmd);
          break;
        default:
          LOG.warn("Unknown command '" + command + "'");
          System.exit(1);
      }
    }
  }

  private static void help(Options options) {
    HelpFormatter formater = new HelpFormatter();
    formater.printHelp("hclient list|create|addpart <options> [name:type...]", options);
    System.exit(0);
  }

  private static void cmdDisplayTables(HMSClient client, CommandLine cmd) throws TException {
    String dbName = cmd.getOptionValue(OPT_DATABASE);
    String tableName = cmd.getOptionValue(OPT_TABLE);
    boolean verbose = cmd.hasOption(OPT_VERBOSE);
    boolean showPartitions = cmd.hasOption(OPT_SHOW_PARTS);

    for (String database : client.getAllDatabases(dbName)) {
      client.getAllTables(database, tableName)
          .stream()
          .sorted()
          .forEach(tblName -> {
            if (verbose) {
              Table table = throwingSupplierWrapper(() -> client.getTable(database, tblName));
              displayTableSchema(table);
              if (showPartitions) {
                System.out.println("\t\t" + Joiner.on("\n\t\t")
                    .join(throwingSupplierWrapper(() ->
                        client.getPartitionNames(database, tblName))));
              }
              System.out.println();
            } else {
              System.out.println(database + "." + tblName);
            }
          });
    }
  }

  private static void cmdCreate(HMSClient client, CommandLine cmd, List<String> arguments)
      throws TException {
    String dbName = cmd.getOptionValue(OPT_DATABASE);
    String tableName = cmd.getOptionValue(OPT_TABLE);

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

    String partitionsInfo = cmd.getOptionValue(OPT_PARTITIONS);
    String[] partitions = partitionsInfo == null ? null : partitionsInfo.split(",");
    List<String> partitionInfo = partitions == null ?
        Collections.emptyList() :
        new ArrayList<>(Arrays.asList(partitions));

    if (!multiple) {
      if (dropTableIfExists(client, cmd, dbName, tableName))
        return;

      client.createTable(new Util.TableBuilder(dbName, tableName)
          .withColumns(Util.createSchema(arguments))
          .withPartitionKeys(Util.createSchema(partitionInfo))
          .build());
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

        client.createTable(new Util.TableBuilder(dbName, tableName)
            .withColumns(Util.createSchema(arguments))
            .withPartitionKeys(Util.createSchema(partitionInfo))
            .build());
        tables.add(tbl);
      }
    }
  }

  private static boolean dropTableIfExists(HMSClient client, CommandLine cmd, String dbName,
      String tableName) throws TException {
    if (client.tableExists(dbName, tableName)) {
      if (cmd.hasOption(OPT_DROP)) {
        LOG.warn("Dropping existing table '" + tableName + "'");
        client.dropTable(dbName, tableName);
      } else {
        LOG.warn("Table '" + tableName + "' already exist");
        return true;
      }
    }
    return false;
  }

  private static void cmdAddPart(HMSClient client, CommandLine cmd, List<String> arguments)
      throws TException {
    String dbName = cmd.getOptionValue(OPT_DATABASE);
    String tableName = cmd.getOptionValue(OPT_TABLE);

    if (tableName != null && tableName.contains(".")) {
      String[] parts = tableName.split("\\.");
      dbName = parts[0];
      tableName = parts[1];
    }

    if (cmd.hasOption(OPT_NUMBER)) {
      int nPartitions = Integer.parseInt(cmd.getOptionValue(OPT_NUMBER));
      Util.addManyPartitions(client, dbName, tableName, arguments, nPartitions);
    } else {
      addPartition(client, dbName, tableName, arguments);
    }
  }

  private static String getPrefixedTableName(int i, String tableName) {
    return new StringBuilder("client_")
        .append(i)
        .append("_")
        .append(tableName).toString();
  }

  private static void loadTable(final CommandLine cmd, final int totalPartitions,
      final String dbName, final String tableName, final int preLoadedPartitions) throws Exception {
    List<Partition> partitions = new ArrayList<>(totalPartitions);
    // insert overwrite simulation begins here. It will alter preLoadedPartitions partitions
    // and create (totalPartitions - preLoadedPartitions) new partitions
    try (HMSClient client = new HMSClient(
        Util.getServerUri(cmd.getOptionValue(OPT_HOST), cmd.getOptionValue(OPT_PORT)),
        cmd.getOptionValue(OPT_CONF))) {
      List<String> values = new ArrayList<>(1);
      Table table = client.getTable(dbName, tableName);
      for (int i = 0; i < totalPartitions; i++) {
        values.add(String.valueOf(i));
        Partition partition = new Util.PartitionBuilder(table).withValues(values).build();
        if (i < preLoadedPartitions) {
          //partition is preloaded so treat it as a static partition alter
          client.alterPartition(dbName, tableName, partition);
        } else {
          //add the new dynamic partition
          client.appendPartition(dbName, tableName, values);
        }
        partitions.add(partition);
        values.clear();
      }
      //one alter_partitions call to simulate stats task
      client.alterPartitions(dbName, tableName, partitions);
    }
  }

  private static void loadTableInParallel(final CommandLine cmd, final int totalPartitions,
      final String dbName, final String tableName, final int numClients,
      final int preloadedPartitions) {
    ExecutorService executor = newFixedThreadPool(numClients);
    try {
      List<Future<?>> results = new ArrayList<>();
      //start from 1 till numClients
      for (int i = 1; i <= numClients; i++) {
        final int j = i;
        results.add(executor.submit(() -> {
          try {
            loadTable(cmd, totalPartitions, dbName, getPrefixedTableName(j, tableName), preloadedPartitions);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }));
      }
      // Wait for results
      results.forEach(r -> throwingSupplierWrapper(r::get));
    } finally {
      executor.shutdownNow();
    }
  }

  private static void cmdDrop(HMSClient client, CommandLine cmd, List<String> arguments)
      throws TException {
    String dbName = cmd.getOptionValue(OPT_DATABASE);
    String tableName = cmd.getOptionValue(OPT_TABLE);

    if (tableName != null && tableName.contains(".")) {
      String[] parts = tableName.split("\\.");
      dbName = parts[0];
      tableName = parts[1];
    }
    if (dbName == null || dbName.isEmpty()) {
      System.out.println("Missing database name");
      System.exit(1);
    }
    if (!arguments.isEmpty()) {
      // Drop partition case
      if (tableName == null || tableName.isEmpty()) {
        System.out.println("Missing table name");
        System.exit(1);
      }
      client.dropPartition(dbName, tableName, arguments);
    } else {
      dropTables(client, dbName, tableName);
    }
  }

  /**
   * Rename table
   * @param client
   * @param cmd
   * @param arguments
   * @throws TException
   */
  private static void cmdRename(HMSClient client, CommandLine cmd, List<String> arguments)
      throws TException {
    String dbName = cmd.getOptionValue(OPT_DATABASE);
    String tableName = cmd.getOptionValue(OPT_TABLE);

    if (arguments.isEmpty()) {
      System.out.println("Missing new name for rename");
      System.exit(1);
    }
    if (arguments.size() > 1) {
      System.out.println("too many arguments for rename");
      System.exit(1);
    }

    if (tableName != null && tableName.contains(".")) {
      String[] parts = tableName.split("\\.");
      dbName = parts[0];
      tableName = parts[1];
    }

    if (tableName == null) {
      LOG.warn("Missing table name");
      System.exit(1);
    }
    if (dbName == null) {
      dbName = DBNAME;
    }

    String newName = arguments.get(0);
    String oldTblName = dbName + "." + tableName;
    String newTblName =  dbName + "." + newName;
    LOG.info("Renaming {} to {}", oldTblName, newTblName);
    Table oldTable = client.getTable(dbName, tableName);
    Table newTable = oldTable.deepCopy();
    newTable.setTableName(newName);
    oldTable.getSd().setLocation("");
    client.alterTable(dbName, tableName, newTable);
  }


  private static void dropTables(HMSClient client, String dbName, String tableName)
      throws TException {
    for (String database : client.getAllDatabases(dbName)) {
      client.getAllTables(database, tableName)
          .stream()
          .sorted()
          .forEach(tblName ->
              throwingSupplierWrapper(() -> client.dropTable(dbName, tblName)));
    }
  }

  private static void cmdDropDatabase(HMSClient client, CommandLine cmd) throws TException {
    String dbName = cmd.getOptionValue(OPT_DATABASE);
    if (dbName == null) {
      LOG.warn("Missing database");
      System.exit(1);
    }
    client.dropDatabase(dbName);
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
