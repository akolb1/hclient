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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.akolb.Util.addManyPartitions;
import static com.akolb.Util.createSchema;
import static com.akolb.Util.getServerUri;

// TODO Handle HADOOP_CONF_DIR and HADOOP_HOME

/**
 * Command-line Hive metastore tool.
 */
final class HMSTool {
  private static final Logger LOG = LoggerFactory.getLogger(HMSTool.class);

  private static final String DBNAME = "default";

  static final String OPT_HOST = "host";
  static final String OPT_PORT = "port";
  static final String OPT_PARTITIONS = "partitions";
  static final String OPT_DATABASE = "database";
  static final String OPT_TABLE = "table";
  static final String OPT_DROP = "drop";
  static final String OPT_VERBOSE = "verbose";
  static final String OPT_NUMBER = "number";
  static final String OPT_PATTERN = "pattern";
  static final String OPT_CONF = "conf";
  private static final String OPT_SHOW_PARTS = "showparts";

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
        .addOption("P", OPT_PORT, true, "HMS Server port")
        .addOption("p", OPT_PARTITIONS, true, "partitions list")
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

    String dbName = cmd.getOptionValue(OPT_DATABASE);
    String tableName = cmd.getOptionValue(OPT_TABLE);

    String partitionsInfo = cmd.getOptionValue(OPT_PARTITIONS);
    String[] partitions = partitionsInfo == null ? null : partitionsInfo.split(",");
    List<String> partitionInfo = partitions == null ?
        Collections.emptyList() :
        new ArrayList<>(Arrays.asList(partitions));

    LogUtils.initHiveLog4j();

    Integer port = null;
    if (cmd.getOptionValue(OPT_PORT) != null) {
      port = Integer.parseInt(cmd.getOptionValue(OPT_PORT));
    }
    try (HMSClient client =
             new HMSClient(getServerUri(cmd.getOptionValue(OPT_HOST), port),
                 cmd.getOptionValue(OPT_CONF))) {
      switch (command) {
        case CMD_LIST:
          cmdDisplayTables(client, cmd);
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

            client.createTable(new Util.TableBuilder(dbName, tableName)
                .setColumns(createSchema(arguments))
                .setPartitionKeys(createSchema(partitionInfo))
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
                  .setColumns(createSchema(arguments))
                  .setPartitionKeys(createSchema(partitionInfo))
                  .build());
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
            addManyPartitions(client, dbName, tableName, arguments, nPartitions);
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
              Table table = client.getTableNoException(database, tblName);
              displayTableSchema(table);
              if (showPartitions) {
                System.out.println("\t\t" + Joiner.on("\n\t\t")
                    .join(client.getPartitionNamesNoException(database, tblName)));
              }
              System.out.println();
            } else {
              System.out.println(database + "." + tblName);
            }
          });
    }
  }

  private static void dropTables(HMSClient client, String dbName, String tableName)
      throws TException {
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
