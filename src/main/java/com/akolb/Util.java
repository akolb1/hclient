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
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;

final class Util {
  private static final String DEFAULT_TYPE = "string";
  private static final String TYPE_SEPARATOR = ":";
  private static final String THRIFT_SCHEMA = "thrift";
  private static final int DEFAULT_PORT = 9083;

  static class TableBuilder {
    private final String dbName;
    private final String tableName;
    private TableType tableType = MANAGED_TABLE;
    private String serde = LazySimpleSerDe.class.getName();
    private List<FieldSchema> columns;
    private List<FieldSchema> partitionKeys;
    private String inputFormat = HiveInputFormat.class.getName();
    private String outputFormat = HiveOutputFormat.class.getName();

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

    TableBuilder setTableType(TableType tabeType) {
      this.tableType = tabeType;
      return this;
    }

    TableBuilder setColumns(List<FieldSchema> columns) {
      this.columns = columns;
      return this;
    }

    TableBuilder setPartitionKeys(List<FieldSchema> partitionKeys) {
      this.partitionKeys = partitionKeys;
      return this;
    }

    TableBuilder setSerde(String serde) {
      this.serde = serde;
      return this;
    }

    TableBuilder setInputFormat(String inputFormat) {
      this.inputFormat = inputFormat;
      return this;
    }

    TableBuilder setOutputFormat(String outputFormat) {
      this.outputFormat = outputFormat;
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

      Table table = new Table();
      table.setDbName(dbName);
      table.setTableName(tableName);
      table.setSd(sd);
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

    PartitionBuilder setLocation(String location) {
      this.location = location;
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

  static @Nullable URI getServerUri(@Nullable String host) throws URISyntaxException {
    return getServerUri(host, DEFAULT_PORT);
  }

  static @Nullable URI getServerUri(@Nullable String host, @Nullable Integer port) throws
      URISyntaxException {
    if (host == null) {
      return null;
    }

    if (port == null) {
      port = DEFAULT_PORT;
    }

    return new URI(THRIFT_SCHEMA, null, host, port,
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

  static void addManyPartitions(@NotNull HMSClient client,
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
  }

  static void addManyPartitionsNoException(@NotNull HMSClient client,
                                           @NotNull String dbName,
                                           @NotNull String tableName,
                                           List<String> arguments,
                                           int npartitions) {
    try {
      addManyPartitions(client, dbName, tableName, arguments, npartitions);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }
}
