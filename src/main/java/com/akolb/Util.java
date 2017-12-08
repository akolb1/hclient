package com.akolb;

import com.google.common.base.Joiner;
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

import javax.annotation.Nonnull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class Util {
  private static final String DEFAULT_TYPE = "string";
  private static final String TYPE_SEPARATOR = ":";
  private static final String THRIFT_SCHEMA = "thrift";
  private static final int DEFAULT_PORT = 9083;

  /**
   * Create table schema from parameters
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
    if (host == null) {
      return null;
    }

    return new URI(THRIFT_SCHEMA, null, host, DEFAULT_PORT,
        null, null, null);
  }

  /**
   * Create Table objects
   *
   * @param dbName    database name
   * @param tableName table name
   * @param columns   table schema
   * @return Table object
   */
  static @NotNull
  Table makeTable(@NotNull String dbName, @NotNull String tableName,
                  @Nullable List<FieldSchema> columns,
                  @Nullable List<FieldSchema> partitionKeys) {
    StorageDescriptor sd = new StorageDescriptor();
    if (columns == null) {
      sd.setCols(Collections.emptyList());
    } else {
      sd.setCols(columns);
    }
    SerDeInfo serdeInfo = new SerDeInfo();
    serdeInfo.setSerializationLib(LazySimpleSerDe.class.getName());
    serdeInfo.setName(tableName);
    sd.setSerdeInfo(serdeInfo);
    sd.setInputFormat(HiveInputFormat.class.getName());
    sd.setOutputFormat(HiveOutputFormat.class.getName());

    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setSd(sd);
    if (partitionKeys != null) {
      table.setPartitionKeys(partitionKeys);
    }

    return table;
  }


  static @NotNull
  Partition makePartition(@NotNull Table table, @NotNull List<String> values) {
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
    partition.getSd().setLocation(table.getSd().getLocation() + "/" + Joiner.on("/").join(spec));
    return partition;
  }

  private static FieldSchema param2Schema(@Nonnull String param) {
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
                                List<String> arguments, int nPartitions) throws TException {
    Table table = client.getTable(dbName, tableName);
    client.createPartitions(
        IntStream.range(0, nPartitions)
            .mapToObj(i ->
                makePartition(table,
                    arguments.stream()
                        .map(a -> a + i)
                        .collect(Collectors.toList())))
            .collect(Collectors.toList()));
  }

  static void addManyPartitionsNoException(@NotNull HMSClient client,
                                @NotNull String dbName,
                                @NotNull String tableName,
                                List<String> arguments, int nPartitions) {
    try {
      addManyPartitions(client, dbName, tableName, arguments, nPartitions);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }
}
