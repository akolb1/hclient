package com.akolb;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Main {
    private static Logger LOG = Logger.getLogger(Main.class.getName());
    // Default column type
    private static final String DEFAULT_TYPE = "string";
    private static final String TYPE_SEPARATOR = ":";

    private static final String DEFAULT_HOST = "localhost";
    private static final String THRIFT_PREFIX = "thrift://";
    private static final String DEFAULT_PORT = "9083";

    private static final String DBNAME = "default";

    private static final String OPT_SERVER = "server";
    private static final String OPT_PORT = "port";
    private static final String OPT_PARTITIONS = "partitions";
    private static final String OPT_DATABASE = "database";
    private static final String OPT_TABLE = "table";
    private static final String OPT_DROP = "drop";
    private static final String OPT_VERBOSE = "verbose";


    private static final String ENV_SERVER = "HMS_THRIFT_SERVER";

    private static final String CMD_LIST = "list";
    private static final String CMD_CREATE = "create";


    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("s", OPT_SERVER, true, "HMS Server")
                .addOption("p", OPT_PORT, true, "port")
                .addOption("P", OPT_PARTITIONS, true, "partitions list")
                .addOption("h", "help", false, "print this info")
                .addOption("d", OPT_DATABASE, true, "database name")
                .addOption("t", OPT_TABLE, true, "table name")
                .addOption("v", OPT_VERBOSE, false, "verbose mode")
                .addOption("D", OPT_DROP, false, "drop table if exists");

        CommandLineParser parser = new DefaultParser();

        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            help(options);
        }

        String server = getServerUri(cmd);

        LOG.info("connecting to {}" + server);

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

        try (HMSClient client = new HMSClient(server)) {
            switch (command) {
                case CMD_LIST:
                    String dbMatcher = dbName == null ? ".*" : dbName;
                    String tableMatcher = tableName == null ? ".*" : tableName;
                    boolean verbose = cmd.hasOption(OPT_VERBOSE);

                    List<String> databases =
                        client.getAllDatabases()
                                .stream()
                                .filter(n -> n.matches(dbMatcher))
                                .collect(Collectors.toList());
                    for (String database: databases) {
                        client.getAllTables(database)
                                .stream()
                                .filter(n -> n.matches(tableMatcher))
                                .forEach(n -> {
                                    if (verbose) {
                                        client.displayTable(database, n);
                                    } else {
                                        System.out.println(database + "." + n);
                                    }
                                });
                    }
                    break;

                case CMD_CREATE:
                    if (tableName != null && tableName.contains(".")) {
                        String[] parts = tableName.split("\\.");
                        dbName = parts[0];
                        tableName = parts[1];
                    }

                    if (dbName == null) {
                        dbName = DBNAME;
                    }

                    if (tableName == null) {
                        LOG.warning("Missing table name");
                        System.exit(1);
                    }

                    if (!client.dbExists(dbName))
                        client.createDatabase(dbName);
                    else {
                        LOG.warning("Database '" + dbName + "' already exist");
                    }

                    if (client.tableExists(dbName, tableName)) {
                        if (cmd.hasOption(OPT_DROP)) {
                            LOG.info("Dropping existing table '" + tableName + "'");
                            client.dropTable(dbName, tableName);
                        } else {
                            LOG.warning("Table '" + tableName + "' already exist");
                            break;
                        }
                    }

                    client.createTable(client.makeTable(dbName, tableName,
                            createSchema(arguments),
                            createSchema(partitionInfo)));
                    LOG.info("Created table '" + tableName + "'");
                    client.displayTable(dbName, tableName);
                    break;
                default:
                    LOG.warning("Unknown command '" + command + "'");
                    System.exit(1);
            }
        }
    }

    private static void help(Options options) {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("hclient <options> [name:type...]", options);
        System.exit(0);
    }

    private static String getServerUri(CommandLine cmd) {
        Map<String, String> env = System.getenv();
        String defaultServer = env.get(ENV_SERVER);
        if (defaultServer == null) {
            defaultServer = DEFAULT_HOST;
        }

        String server = cmd.getOptionValue(OPT_SERVER, defaultServer);
        if (!server.contains(":")) {
            String port = cmd.getOptionValue(OPT_PORT, DEFAULT_PORT);
            server = server + ":" + port;
        }
        if (!server.startsWith(THRIFT_PREFIX)) {
            server = THRIFT_PREFIX + server;
        }

        return server;
    }

    /**
     * Create table schema from parameters
     * @param params list of parameters. Each parameter can be either a simple name or
     *               name:type for non-String types.
     * @return table schema description
     */
    private static List<FieldSchema> createSchema(List<String> params) {
        if (params == null || params.isEmpty()) {
            return Collections.emptyList();
        }

        ArrayList<FieldSchema> cols = new ArrayList<>(params.size());
        for (String param: params) {
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


}
