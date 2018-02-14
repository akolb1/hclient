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
import com.google.common.net.HostAndPort;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.akolb.HMSClient.throwingSupplierWrapper;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;

final class Util {
  private static final String DEFAULT_TYPE = "string";
  private static final String TYPE_SEPARATOR = ":";
  private static final String THRIFT_SCHEMA = "thrift";
  static final String DEFAULT_HOST = "localhost";
  private static final int DEFAULT_PORT = 9083;
  private static final String ENV_SERVER = "HMS_HOST";
  private static final String ENV_PORT = "HMS_PORT";
  private static final String PROP_HOST = "hms.host";
  private static final String PROP_PORT = "hms.port";

  private static final Logger LOG = LoggerFactory.getLogger(Util.class);

  /**
   * A builder for Database.  The name of the new database is required.  Everything else
   * selects reasonable defaults.
   * This is a modified version of Hive 3.0 DatabaseBuilder.
   */
  public static class DatabaseBuilder {
    private String name;
    private String description;
    private String location;
    private String ownerName;
    private PrincipalType ownerType;
    private Map<String, String> params = null;

    private DatabaseBuilder() {
    }

    public DatabaseBuilder(String name) {
      this.name = name;
      ownerType = PrincipalType.USER;
    }

    public DatabaseBuilder withDescription(String description) {
      this.description = description;
      return this;
    }

    public DatabaseBuilder withLocation(String location) {
      this.location = location;
      return this;
    }

    public DatabaseBuilder withParams(Map<String, String> params) {
      this.params = params;
      return this;
    }

    public DatabaseBuilder withParam(String key, String val) {
      if (this.params == null) {
        this.params = new HashMap<>();
      }
      this.params.put(key, val);
      return this;
    }

    public DatabaseBuilder withOwnerName(String ownerName) {
      this.ownerName = ownerName;
      return this;
    }

    public DatabaseBuilder withOwnerType(PrincipalType ownerType) {
      this.ownerType = ownerType;
      return this;
    }

    public Database build() {
      Database db = new Database(name, description, location, params);
      if (ownerName != null) {
        db.setOwnerName(ownerName);
      }
      if (ownerType != null) {
        db.setOwnerType(ownerType);
      }
      return db;
    }
  }

  static class TableBuilder {
    private final String dbName;
    private final String tableName;
    private TableType tableType = MANAGED_TABLE;
    private String location;
    private String serde = LazySimpleSerDe.class.getName();
    private String owner;
    private List<FieldSchema> columns;
    private List<FieldSchema> partitionKeys;
    private String inputFormat = HiveInputFormat.class.getName();
    private String outputFormat = HiveOutputFormat.class.getName();
    private Map<String, String> parameters = new HashMap<>();

    private TableBuilder() {
      dbName = null;
      tableName = null;
    }

    TableBuilder(String dbName, String tableName) {
      this.dbName = dbName;
      this.tableName = tableName;
    }

    static Table buildDefaultTable(String dbName, String tableName) {
      return new TableBuilder(dbName, tableName).build();
    }

    TableBuilder withType(TableType tabeType) {
      this.tableType = tabeType;
      return this;
    }

    TableBuilder withOwner(String owner) {
      this.owner = owner;
      return this;
    }

    TableBuilder withColumns(List<FieldSchema> columns) {
      this.columns = columns;
      return this;
    }

    TableBuilder withPartitionKeys(List<FieldSchema> partitionKeys) {
      this.partitionKeys = partitionKeys;
      return this;
    }

    TableBuilder withSerde(String serde) {
      this.serde = serde;
      return this;
    }

    TableBuilder withInputFormat(String inputFormat) {
      this.inputFormat = inputFormat;
      return this;
    }

    TableBuilder withOutputFormat(String outputFormat) {
      this.outputFormat = outputFormat;
      return this;
    }

    TableBuilder withParameter(String name, String value) {
      parameters.put(name, value);
      return this;
    }

    TableBuilder withLocation(String location) {
      this.location = location;
      return this;
    }

    Table build() {
      StorageDescriptor sd = new StorageDescriptor();
      if (columns == null) {
        sd.setCols(Collections.emptyList());
      } else {
        sd.setCols(columns);
      }
      SerDeInfo serdeInfo = new SerDeInfo();
      serdeInfo.setSerializationLib(serde);
      serdeInfo.setName(tableName);
      sd.setSerdeInfo(serdeInfo);
      sd.setInputFormat(inputFormat);
      sd.setOutputFormat(outputFormat);
      if (location != null) {
        sd.setLocation(location);
      }

      Table table = new Table();
      table.setDbName(dbName);
      table.setTableName(tableName);
      table.setSd(sd);
      table.setParameters(parameters);
      table.setOwner(owner);
      if (partitionKeys != null) {
        table.setPartitionKeys(partitionKeys);
      }
      table.setTableType(tableType.toString());
      return table;
    }
  }

  static class PartitionBuilder {
    private final Table table;
    private List<String> values;
    private String location;
    private Map<String, String> parameters = new HashMap<>();

    private PartitionBuilder() {
      table = null;
    }

    PartitionBuilder(Table table) {
      this.table = table;
    }

    PartitionBuilder setValues(List<String> values) {
      this.values = values;
      return this;
    }

    PartitionBuilder withLocation(String location) {
      this.location = location;
      return this;
    }

    PartitionBuilder withParameter(String name, String value) {
      parameters.put(name, value);
      return this;
    }

    Partition build() {
      Partition partition = new Partition();
      List<String> partitionNames = table.getPartitionKeys()
          .stream()
          .map(FieldSchema::getName)
          .collect(Collectors.toList());
      if (partitionNames.size() != values.size()) {
        throw new RuntimeException("Partition values do not match table schema");
      }
      List<String> spec = IntStream.range(0, values.size())
          .mapToObj(i -> partitionNames.get(i) + "=" + values.get(i))
          .collect(Collectors.toList());

      partition.setDbName(table.getDbName());
      partition.setTableName(table.getTableName());
      partition.setParameters(parameters);
      partition.setValues(values);
      partition.setSd(table.getSd().deepCopy());
      if (this.location == null) {
        partition.getSd().setLocation(table.getSd().getLocation() + "/" + Joiner.on("/").join(spec));
      } else {
        partition.getSd().setLocation(location);
      }
      return partition;
    }
  }

  /**
   * Create table schema from parameters.
   *
   * @param params list of parameters. Each parameter can be either a simple name or
   *               name:type for non-String types.
   * @return table schema description
   */
  static List<FieldSchema> createSchema(@Nullable List<String> params) {
    if (params == null || params.isEmpty()) {
      return Collections.emptyList();
    }

    return params.stream()
        .map(Util::param2Schema)
        .collect(Collectors.toList());
  }

  /**
   * Get server URI.<p>
   * <p>
   * HMS host is obtained from
   * <ol>
   * <li>Argument</li>
   * <li>HMS_HOST environment parameter</li>
   * <li>hms.host Java property</li>
   * <li>use 'localhost' if above fails</li>
   * </ol>
   * HMS Port is obtained from
   * <ol>
   * <li>Argument</li>
   * <li>host:port string</li>
   * <li>HMS_PORT environment variable</li>
   * <li>hms.port Java property</li>
   * <li>default port value</li>
   * </ol>
   *
   * @param host       HMS host string.
   * @param portString HMS port
   * @return HMS URI
   * @throws URISyntaxException
   */
  static @Nullable URI getServerUri(@Nullable String host, @Nullable String portString) throws
      URISyntaxException {
    if (host == null) {
      host = System.getenv(ENV_SERVER);
    }
    if (host == null) {
      host = System.getProperty(PROP_HOST);
    }
    if (host == null) {
      host = DEFAULT_HOST;
    }
    if (portString == null && !host.contains(":")) {
      portString = System.getenv(ENV_PORT);
      if (portString == null) {
        portString = System.getProperty(PROP_PORT);
      }
    }
    Integer port = DEFAULT_PORT;
    if (portString != null) {
      port = Integer.parseInt(portString);
    }

    HostAndPort hp = HostAndPort.fromString(host)
        .withDefaultPort(port);

    LOG.info("Connecting to {}:{}", hp.getHostText(), hp.getPort());

    return new URI(THRIFT_SCHEMA, null, hp.getHostText(), hp.getPort(),
        null, null, null);
  }


  private static FieldSchema param2Schema(@NotNull String param) {
    String colType = DEFAULT_TYPE;
    String name = param;
    if (param.contains(TYPE_SEPARATOR)) {
      String[] parts = param.split(TYPE_SEPARATOR);
      name = parts[0];
      colType = parts[1].toLowerCase();
    }
    return new FieldSchema(name, colType, "");
  }

  static Object addManyPartitions(@NotNull HMSClient client,
                                @NotNull String dbName,
                                @NotNull String tableName,
                                List<String> arguments,
                                int npartitions) throws TException {
    Table table = client.getTable(dbName, tableName);
    client.createPartitions(
        IntStream.range(0, npartitions)
            .mapToObj(i ->
                new PartitionBuilder(table)
                    .setValues(
                        arguments.stream()
                            .map(a -> a + i)
                            .collect(Collectors.toList())).build())
            .collect(Collectors.toList()));
    return null;
  }

  static void addManyPartitionsNoException(@NotNull HMSClient client,
                                           @NotNull String dbName,
                                           @NotNull String tableName,
                                           List<String> arguments,
                                           int npartitions) {
    throwingSupplierWrapper(() ->
        addManyPartitions(client, dbName, tableName, arguments, npartitions));
  }
}
