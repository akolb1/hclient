package com.akolb;

import com.google.common.base.Joiner;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HMSClient implements AutoCloseable {
  private static final String METASTORE_URI = "hive.metastore.uris";

  private final HiveMetaStoreClient client;

  HMSClient(@Nonnull String server) throws MetaException {
    client = getClient(server);
  }

  private HiveMetaStoreClient getClient(@Nonnull String server) throws MetaException {
    HiveConf conf = new HiveConf();
    conf.set(METASTORE_URI, server);
    return new HiveMetaStoreClient(conf);
  }

  boolean dbExists(@Nonnull String dbName) throws MetaException {
    return getAllDatabases(dbName).contains(dbName);
  }

  boolean tableExists(@Nonnull String dbName, @Nonnull String tableName) throws MetaException {
    return getAllTables(dbName, tableName).contains(tableName);
  }

  /**
   * Return all databases with name matching the filter
   *
   * @param filter Regexp. Can be null or empty in which case everything matches
   * @return list of database names matching the filter
   * @throws MetaException
   */
  Set<String> getAllDatabases(@Nullable String filter) throws MetaException {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.getAllDatabases());
    }
    return client.getAllDatabases()
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  List<String> getAllDatabasesNoException() {
    try {
      return client.getAllDatabases();
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  Set<String> getAllTables(@Nonnull String dbName, @Nullable String filter) throws MetaException {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.getAllTables(dbName));
    }
    return client.getAllTables(dbName)
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  List<String> getAllTablesNoException(@NonNull String dbName) {
    try {
      return client.getAllTables(dbName);
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create database with th egiven name if it doesn't exist
   *
   * @param name database name
   */
  void createDatabase(@Nonnull String name) throws TException {
    Database db = new Database();
    db.setName(name);
    client.createDatabase(db);
  }

  void createTable(Table table) throws TException {
    client.createTable(table);
  }

  /**
   * Create tabe but convert any exception to unchecked {@link RuntimeException}
   *
   * @param table table to create
   */
  void createTableNoException(@Nonnull Table table) {
    try {
      client.createTable(table);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  void dropTable(@Nonnull String dbName, @Nonnull String tableName) throws TException {
    client.dropTable(dbName, tableName);
  }

  /**
   * Drop table but convert any exception to unchecked {@link RuntimeException}.
   *
   * @param dbName    Database name
   * @param tableName Table name
   */
  void dropTableNoException(@Nonnull String dbName, @Nonnull String tableName) {
    try {
      client.dropTable(dbName, tableName);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }


  Table getTable(@Nonnull String dbName, @Nonnull String tableName) throws TException {
    return client.getTable(dbName, tableName);
  }

  Table getTableNoException(@Nonnull String dbName, @Nonnull String tableName) {
    try {
      return client.getTable(dbName, tableName);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create Table objects
   *
   * @param dbName    database name
   * @param tableName table name
   * @param columns   table schema
   * @return Table object
   */
  static @Nonnull
  Table makeTable(@Nonnull String dbName, @Nonnull String tableName,
                  @Nullable List<FieldSchema> columns,
                  @Nullable List<FieldSchema> partitionKeys) {
    StorageDescriptor sd = new StorageDescriptor();
    if (columns == null) {
      sd.setCols(Collections.emptyList());
    } else {
      sd.setCols(columns);
    }
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tableName);

    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setSd(sd);
    if (partitionKeys != null) {
      table.setPartitionKeys(partitionKeys);
    }
    SerDeInfo serdeInfo = new SerDeInfo();
    serdeInfo.setSerializationLib(LazySimpleSerDe.class.getName());
    sd.setSerdeInfo(serdeInfo);
    sd.setInputFormat(HiveInputFormat.class.getName());
    sd.setOutputFormat(HiveOutputFormat.class.getName());

    return table;
  }

  static @Nonnull
  Partition makePartition(@Nonnull Table table, @Nonnull List<String> values) {
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

  void createPartition(@Nonnull Table table, @Nonnull List<String> values) throws TException {
    client.add_partition(makePartition(table, values));
  }

  void createPartitionNoException(@Nonnull Table table, @Nonnull List<String> values) {
    try {
      client.add_partition(makePartition(table, values));
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  void createPartitionNoException(@Nonnull Partition partition) {
    try {
      client.add_partition(partition);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  List<Partition> listPartitions(String dbName, String tableName) throws TException {
    return client.listPartitions(dbName, tableName, (short)-1);
  }

  List<Partition> listPartitionsNoException(String dbName, String tableName) {
    try {
      return client.listPartitions(dbName, tableName, (short)-1);
    } catch (TException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  public boolean dropPartition(String dbName, String tableName, List<String> arguments)
      throws TException {
    return client.dropPartition(dbName, tableName, arguments);
  }
  public boolean dropPartitionNoException(String dbName, String tableName, List<String> arguments) {
    try {
      return client.dropPartition(dbName, tableName, arguments);
    } catch (TException e) {
      throw  new RuntimeException(e);
    }
  }
}
