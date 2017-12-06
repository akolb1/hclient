package com.akolb;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginContext;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class HMSClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(HMSClient.class);
  private static final String METASTORE_URI = "hive.metastore.uris";
  private static final String CONFIG_DIR = "/etc/hive/conf";
  private static final String HIVE_SITE = "hive-site.xml";
  private static final String CORE_SITE = "core-site.xml";
  private static final String PRINCIPAL_KEY = "hive.metastore.kerberos.principal";

  private final HiveMetaStoreClient client;
  private final String confDir;

  HMSClient(@Nullable URI uri)
      throws MetaException, IOException, InterruptedException {
    this(uri, CONFIG_DIR);
  }

  HMSClient(@Nullable URI uri, @Nullable String confDir)
      throws MetaException, IOException, InterruptedException {
    this.confDir = (confDir == null ? CONFIG_DIR : confDir);
    client = getClient(uri);
  }

  private void addResource(Configuration conf, @NotNull String r) throws MalformedURLException {
    File f = new File(confDir + "/" + r);
    if (f.exists() && !f.isDirectory()) {
      LOG.debug("Adding configuration resource {}", r);
      conf.addResource(f.toURI().toURL());
    } else {
      LOG.debug("Configuration {} does not exist", r);
    }
  }

  /**
   * Create a client to Hive Metastore.
   * If principal is specified, create kerberised client.
   *
   * @param uri server uri
   * @return {@link HiveMetaStoreClient} usable for talking to HMS
   * @throws MetaException        if fails to login using kerberos credentials
   * @throws IOException          if fails connecting to metastore
   * @throws InterruptedException if interrupted during kerberos setup
   */
  private HiveMetaStoreClient getClient(@Nullable URI uri)
      throws MetaException, IOException, InterruptedException {
    HiveConf conf = new HiveConf();
    addResource(conf, HIVE_SITE);
    if (uri != null) {
      conf.set(METASTORE_URI, uri.toString());
    }
    if (conf.get(METASTORE_URI) == null) {
      conf.set(METASTORE_URI, "localhost");
    }

    LOG.info("connecting to {}", conf.get(METASTORE_URI));

    String principal = conf.get(PRINCIPAL_KEY);

    if (principal == null) {
      new HiveMetaStoreClient(conf);
    }

    LOG.debug("Opening kerberos connection to HMS");
    addResource(conf, CORE_SITE);

    Configuration hadoopConf = new Configuration();
    addResource(hadoopConf, HIVE_SITE);
    addResource(hadoopConf, CORE_SITE);

    // Kerberos magic
    UserGroupInformation.setConfiguration(hadoopConf);
    UserGroupInformation userUGI = UserGroupInformation.getLoginUser();
    return userUGI.doAs((PrivilegedExceptionAction<HiveMetaStoreClient>) () ->
        new HiveMetaStoreClient(conf));
  }

  boolean dbExists(@NotNull String dbName) throws MetaException {
    return getAllDatabases(dbName).contains(dbName);
  }

  boolean tableExists(@NotNull String dbName, @NotNull String tableName) throws MetaException {
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

  Set<String> getAllTables(@NotNull String dbName, @Nullable String filter) throws MetaException {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.getAllTables(dbName));
    }
    return client.getAllTables(dbName)
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  List<String> getAllTablesNoException(@NotNull String dbName) {
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
  void createDatabase(@NotNull String name) throws TException {
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
  void createTableNoException(@NotNull Table table) {
    try {
      client.createTable(table);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  void dropTable(@NotNull String dbName, @NotNull String tableName) throws TException {
    client.dropTable(dbName, tableName);
  }

  /**
   * Drop table but convert any exception to unchecked {@link RuntimeException}.
   *
   * @param dbName    Database name
   * @param tableName Table name
   */
  void dropTableNoException(@NotNull String dbName, @NotNull String tableName) {
    try {
      client.dropTable(dbName, tableName);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }


  Table getTable(@NotNull String dbName, @NotNull String tableName) throws TException {
    return client.getTable(dbName, tableName);
  }

  Table getTableNoException(@NotNull String dbName, @NotNull String tableName) {
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

  void createPartition(@NotNull Table table, @NotNull List<String> values) throws TException {
    client.add_partition(makePartition(table, values));
  }

  private void createPartitions(List<Partition> partitions) throws TException {
    client.add_partitions(partitions);
  }

  void createPartitionNoException(@NotNull Partition partition) {
    try {
      client.add_partition(partition);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  List<Partition> listPartitions(String dbName, String tableName) throws TException {
    return client.listPartitions(dbName, tableName, (short) -1);
  }

  List<Partition> listPartitionsNoException(String dbName, String tableName) {
    try {
      return client.listPartitions(dbName, tableName, (short) -1);
    } catch (TException e) {
      LOG.error("Failed to list partitions", e);
      throw new RuntimeException(e);
    }
  }

  void addManyPartitions(String dbName, String tableName,
                         List<String> arguments, int nPartitions) throws TException {
    Table table = client.getTable(dbName, tableName);
    createPartitions(
        IntStream.range(0, nPartitions)
            .mapToObj(i ->
                makePartition(table,
                    arguments.stream()
                        .map(a -> a + i)
                        .collect(Collectors.toList())))
            .collect(Collectors.toList()));
  }

  long getCurrentNotificationId() throws TException {
    return client.getCurrentNotificationEventId().getEventId();
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
      throw new RuntimeException(e);
    }
  }
}
