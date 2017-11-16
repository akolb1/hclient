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

import java.util.Collection;
import java.util.List;

public class HMSClient implements AutoCloseable {
  private static final String METASTORE_URI = "hive.metastore.uris";

  private final HiveMetaStoreClient client;

  HMSClient(String server) throws MetaException {
    client = getClient(server);
  }

  private HiveMetaStoreClient getClient(String server) throws MetaException {
    HiveConf conf = new HiveConf();
    conf.set(METASTORE_URI, server);
    return new HiveMetaStoreClient(conf);
  }

  private static boolean has(Collection<String> elements, String value) {
    return elements.stream().anyMatch(n -> n.equalsIgnoreCase(value));
  }

  boolean dbExists(String dbName) throws MetaException {
    return has(client.getAllDatabases(), dbName);
  }

  boolean tableExists(String dbName, String tableName) throws MetaException {
    return has(client.getAllTables(dbName), tableName);
  }

  /**
   * Create database with th egiven name if it doesn't exist
   * @param name database name
   */
  void createDatabase(String name) throws TException {
    Database db = new Database();
    db.setName(name);
    client.createDatabase(db);
  }

  void createTable(Table table) throws TException {
    client.createTable(table);
  }

  void dropTable(String dbName, String tableName) throws TException {
    client.dropTable(dbName, tableName);
  }

  Table getTable(String dbName, String tableName) throws TException {
    return client.getTable(dbName, tableName);
  }

  /**
   * Create Table objects
   * @param dbName database name
   * @param tableName table name
   * @param columns table schema
   * @return Table object
   */
  Table makeTable(String dbName, String tableName, List<FieldSchema> columns, List<FieldSchema> partitionKeys) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(columns);
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tableName);

    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setSd(sd);
    table.setPartitionKeys(partitionKeys);
    return table;
  }

  void printTable(Table table) {
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

  @Override
  public void close() throws Exception {
    client.close();
  }
}
