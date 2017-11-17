package com.akolb;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

  Set<String> getAllTables(@Nonnull String dbName, @Nullable String filter) throws MetaException {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.getAllTables(dbName));
    }
    return client.getAllTables(dbName)
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  /**
   * Create database with th egiven name if it doesn't exist
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
   * @param dbName Database name
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

  /**
   * Create Table objects
   * @param dbName database name
   * @param tableName table name
   * @param columns table schema
   * @return Table object
   */
  static Table makeTable(@Nonnull String dbName, @Nonnull String tableName,
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
    return table;
  }

  static void printTable(@Nonnull Table table) {
    String dbName = table.getDbName();
    String tableName = table.getTableName();
    List<FieldSchema> columns = table.getSd().getCols();
    System.out.println(dbName + "." + tableName);
    for (FieldSchema schema: columns) {
      System.out.println("\t" + schema.getName() + ":\t" + schema.getType());
    }
    List<FieldSchema> partitions = table.getPartitionKeys();
    for (FieldSchema schema: partitions) {
      System.out.println("\t  " + schema.getName() + ":\t" + schema.getType());
    }
  }

  void displayTable(@Nonnull String dbName, @Nonnull String tableName) {
    try {
      printTable(getTable(dbName, tableName));
      System.out.println();
    } catch (TException e) {
      System.out.println(dbName + "." + tableName + ": " + e.getMessage());
    }
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
