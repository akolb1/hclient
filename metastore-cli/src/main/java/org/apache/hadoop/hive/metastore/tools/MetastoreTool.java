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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import picocli.CommandLine;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.tools.Constants.HMS_DEFAULT_PORT;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.HelpCommand;
import static picocli.CommandLine.Option;
import static picocli.CommandLine.ParentCommand;

/**
 * Command-line access to Hive Metastore.
 */
@SuppressWarnings({"squid:S106", "squid:S1148"}) // Using System.out
@Command(name = "MetastoreTool",
        mixinStandardHelpOptions = true, version = "1.0",
        subcommands = {HelpCommand.class,
                MetastoreTool.DbCommand.class,
                MetastoreTool.TableCommand.class},
        showDefaultValues = true)

public class MetastoreTool implements Runnable {
  @Option(names = {"-H", "--host"},
          description = "HMS Host",
          paramLabel = "URI")
  private String host;
  @Option(names = {"-P", "--port"}, description = "HMS Server port")
  private Integer port = HMS_DEFAULT_PORT;

  public static void main(String[] args) {
    CommandLine.run(new MetastoreTool(), args);
  }

  @Override
  public void run() {
    try (HMSClient client = new HMSClient(Util.getServerUri(host, String.valueOf(port)))) {
      TableCommand.printTableList(client, null, null);
    } catch (URISyntaxException e) {
      System.out.println("invalid host " + host);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Command(name = "db",
          subcommands = {HelpCommand.class, DbCommand.DbListCommand.class, DbCommand.DbShowCommand.class})
  static class DbCommand implements Runnable {

    @ParentCommand
    private MetastoreTool parent;

    @Option(names = {"-d", "--db"}, description = "Database name pattern")
    private String dbName;

    String getHost() {
      return parent.host;
    }

    int getPort() {
      return parent.port;
    }

    String getDbName() {
      return dbName;
    }

    static void printDbList(HMSClient client, String dbName) throws TException {
      client.getAllDatabases(dbName).forEach(System.out::println);
    }

    @Override
    public void run() {
      String host = getHost();
      int port = getPort();
      try (HMSClient client = new HMSClient(Util.getServerUri(host, String.valueOf(port)))) {
        printDbList(client, dbName);
      } catch (URISyntaxException e) {
        System.out.println("invalid host " + host);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    /**
     * List databases.
     */
    @Command(name = "list",
            description = "List all databases, optionally matching pattern")
    static class DbListCommand implements Runnable {

      @ParentCommand
      private DbCommand parent;

      @Override
      public void run() {
        String host = parent.getHost();
        int port = parent.getPort();
        String dbName = parent.getDbName();

        try (HMSClient client = new HMSClient(Util.getServerUri(host, String.valueOf(port)))) {
          printDbList(client, dbName);
        } catch (URISyntaxException e) {
          System.out.println("invalid host " + host);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    @Command(name = "show",
            description = "Show all databases, optionally matching pattern")
    static class DbShowCommand implements Runnable {

      @ParentCommand
      private DbCommand parent;

      @Override
      public void run() {
        String host = parent.getHost();
        int port = parent.getPort();
        String dbName = parent.getDbName();

        try (HMSClient client = new HMSClient(Util.getServerUri(host, String.valueOf(port)))) {
          Set<String> dbNames = client.getAllDatabases(dbName);
          List<Database> databases = new ArrayList<>(dbNames.size());
          for (String name : dbNames) {
            databases.add(client.getDatabase(name));
          }
          Gson gson = new GsonBuilder().setPrettyPrinting().create();
          System.out.println(gson.toJson(databases,
                  new TypeToken<List<Database>>() {
                  }.getType()));
        } catch (URISyntaxException e) {
          System.out.println("invalid host " + host);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Command(name = "table",
          subcommands = {HelpCommand.class, TableCommand.TablePrintCommand.class},
          description = "HMS Table operations")
  static class TableCommand implements Runnable {

    @ParentCommand
    private MetastoreTool parent;

    @Option(names = {"-d", "--db"}, description = "Database name pattern")
    private String dbName;

    static void printTableList(HMSClient client, String dbName, String tableName) throws TException {
      for (String database : client.getAllDatabases(dbName)) {
        client.getAllTables(database, tableName).stream()
                .sorted()
                .map(t -> database + "." + t)
                .forEach(System.out::println);
      }
    }

    /**
     * List all tables
     */
    @Command(name = "list", description = "List all tables")
    static class TablePrintCommand implements Runnable {

      @ParentCommand
      private TableCommand parent;

      @Option(names = {"-t", "--table"}, description = "Table name pattern")
      private String tableName;

      @Override
      public void run() {
        String host = parent.parent.host;
        int port = parent.parent.port;
        String dbName = parent.dbName;
        try (HMSClient client = new HMSClient(Util.getServerUri(host, String.valueOf(port)))) {
          printTableList(client, dbName, tableName);
        } catch (URISyntaxException e) {
          System.out.println("invalid host " + host);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    @Override
    public void run() {
      String host = parent.host;
      int port = parent.port;
      try (HMSClient client = new HMSClient(Util.getServerUri(host, String.valueOf(port)))) {
        printTableList(client, dbName, null);
      } catch (URISyntaxException e) {
        System.out.println("invalid host " + host);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
