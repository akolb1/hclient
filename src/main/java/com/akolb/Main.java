package com.akolb;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Main {
    private static Logger LOG = Logger.getLogger(Main.class.getName());
    // Default column type
    private static final String DEFAULT_TYPE = "string";
    private static final String TYPE_SEPARATOR = ":";

    static final String DEFAULT_HOST = "localhost";
    static final String THRIFT_SCHEMA = "thrift";
    static final int DEFAULT_PORT = 9083;

    private static final String DBNAME = "default";

    static final String OPT_SERVER = "server";
    static final String OPT_PARTITIONS = "partitions";
    static final String OPT_DATABASE = "database";
    static final String OPT_TABLE = "table";
    static final String OPT_DROP = "drop";
    static final String OPT_VERBOSE = "verbose";
    static final String OPT_NUMBER = "number";
    static final String OPT_PATTERN = "pattern";

    static final String DEFAULT_PATTERN = "%s_%d";
    static final String ENV_SERVER = "HMS_THRIFT_SERVER";

    static final String CMD_LIST = "list";
    static final String CMD_CREATE = "create";


    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("s", OPT_SERVER, true, "HMS Server")
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

        String server = getServerUri(cmd).toString();

        LOG.info("connecting to " + server);

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

        try (HMSClient client = new HMSClient(server)) {
            switch (command) {
                case CMD_LIST:
                    for (String database: client.getAllDatabases(dbName)) {
                        client.getAllTables(database, tableName)
                            .stream()
                            .sorted()
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
                        LOG.warning("Missing table name");
                        System.exit(1);
                    }

                    if (!client.dbExists(dbName))
                        client.createDatabase(dbName);
                    else {
                        LOG.warning("Database '" + dbName + "' already exist");
                    }

                    if (!multiple) {
                        if (client.tableExists(dbName, tableName)) {
                            if (cmd.hasOption(OPT_DROP)) {
                                LOG.warning("Dropping existing table '" + tableName + "'");
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
                    } else {
                        Set<String> tables = client.getAllTables(dbName, null);
                        for (int i = 1; i <= nTables; i++) {
                            String pattern = cmd.getOptionValue(OPT_PATTERN, DEFAULT_PATTERN);
                            String tbl = String.format(pattern, tableName, i);
                            if (tables.contains(tbl)) {
                                if (cmd.hasOption(OPT_DROP)) {
                                    LOG.warning("Dropping existing table '" + tbl + "'");
                                    client.dropTable(dbName, tbl);
                                } else {
                                    LOG.warning("Table '" + tbl + "' already exist");
                                    break;
                                }
                            }

                            client.createTable(client.makeTable(dbName, tbl,
                                    createSchema(arguments),
                                    createSchema(partitionInfo)));
                            tables.add(tbl);
                            if (verbose) {
                                client.displayTable(dbName, tbl);
                            }
                        }
                    }
                    break;
                default:
                    LOG.warning("Unknown command '" + command + "'");
                    System.exit(1);
            }
        }
    }

    static void help(Options options) {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("hclient list|create <options> [name:type...]", options);
        System.exit(0);
    }

    static URI getServerUri(CommandLine cmd) {
        Map<String, String> env = System.getenv();
        String defaultServer = env.get(ENV_SERVER);
        if (defaultServer == null) {
            defaultServer = DEFAULT_HOST;
        }

        String server = cmd.getOptionValue(OPT_SERVER, defaultServer);

        try {
            return new URI(THRIFT_SCHEMA, null, server, DEFAULT_PORT,
                null,null, null);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    /**
     * Create table schema from parameters
     * @param params list of parameters. Each parameter can be either a simple name or
     *               name:type for non-String types.
     * @return table schema description
     */
    static List<FieldSchema> createSchema(List<String> params) {
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
