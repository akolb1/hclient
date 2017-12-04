package com.akolb;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class HMSClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(HMSClient.class.getName());
  private static final String METASTORE_URI = "hive.metastore.uris";

  private final HiveMetaStoreClient client;
  private LoginContext loginContext;

  HMSClient(@NotNull String server) throws MetaException, IOException, InterruptedException {
    client = getClient(server);
  }

  HMSClient(@NotNull String server, @Nullable String principal, @Nullable String keytab)
      throws MetaException, IOException, InterruptedException {
    client = getClient(server, principal, keytab);
  }

  private HiveMetaStoreClient getClient(@NotNull String server) throws MetaException,
      IOException, InterruptedException {
    return getClient(server, null, null);
  }

  /**
   * Create a client to Hive Metastore.
   * If principal is specified, create kerberised client.
   *
   * @param server    server:port
   * @param principal Optional kerberos principal
   * @param keyTab    Optional Kerberos keytab.
   * @return {@link HiveMetaStoreClient} usable for talking to HMS
   * @throws MetaException        if fails to login using kerberos credentials
   * @throws IOException          if fails connecting to metastore
   * @throws InterruptedException if interrupted during kerberos setup
   */
  private HiveMetaStoreClient getClient(@NotNull String server, @Nullable String principal,
                                        @Nullable String keyTab)
      throws MetaException, IOException, InterruptedException {
    HiveConf conf = new HiveConf();
    conf.set(METASTORE_URI, server);
    if (principal == null || principal.isEmpty()) {
      return new HiveMetaStoreClient(conf);
    }
    return getKerberosClient(conf, Preconditions.checkNotNull(principal),
        Preconditions.checkNotNull(keyTab));
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
    if (loginContext != null) {
      loginContext.logout();
      loginContext = null;
    }
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

  /**
   * Borrowed from Sentry KerberosConfiguration.java
   */
  private HiveMetaStoreClient getKerberosClient(HiveConf conf,
                                                @NotNull String principal,
                                                @NotNull String keytab) throws IOException,
      MetaException, InterruptedException {
    String host = "localhost";
    int port = 1234;

    String serverPrincipal = SecurityUtil.getServerPrincipal(principal,
        NetUtils.createSocketAddr(host, port).getAddress());
    LOG.debug("Opening kerberos connection to HMS using kerberos principal {}, serverPrincipal {}",
        principal, serverPrincipal);
    File keytabFile = new File(keytab);
    Preconditions.checkState(keytabFile.isFile() && keytabFile.canRead(),
        "Keytab %s does not exist or is not readable", keytab);
    Subject subject = new Subject(false,
        Sets.newHashSet(new KerberosPrincipal(serverPrincipal)),
        Collections.emptySet(), Collections.emptySet());
    javax.security.auth.login.Configuration kerberosConfig =
        KerberosConfiguration.createClientConfig(principal, keytabFile);

    try {
      loginContext = new LoginContext("", subject, null, kerberosConfig);
      loginContext.login();
    } catch (LoginException e) {
      LOG.info("failed to login with subject {}", subject);
      LOG.error("Failed to login", e);
      throw new MetaException("Can't login via kerberos: " + e.getMessage());
    }
    subject = loginContext.getSubject();
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("hadoop.security.authorization", "true");
    conf.set("hadoop.security.auth_to_local", "DEFAULT");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation clientUGI =
        UserGroupInformation.getUGIFromSubject(subject);
    return clientUGI.doAs((PrivilegedExceptionAction<HiveMetaStoreClient>)
        () -> new HiveMetaStoreClient(conf));
  }


  private static class KerberosConfiguration extends javax.security.auth.login.Configuration {
    private static final String KRBCNAME = "KRB5CCNAME";

    private final String principal;
    private final String keytab;
    private final boolean isInitiator;
    private static final boolean IBM_JAVA = System.getProperty("java.vendor").contains("IBM");

    private KerberosConfiguration(String principal, File keytab,
                                  boolean client) {
      this.principal = principal;
      this.keytab = keytab.getAbsolutePath();
      this.isInitiator = client;
    }

    static javax.security.auth.login.Configuration createClientConfig(String principal,
                                                                      File keytab) {
      return new KerberosConfiguration(principal, keytab, true);
    }

    public static javax.security.auth.login.Configuration createServerConfig(String principal,
                                                                             File keytab) {
      return new KerberosConfiguration(principal, keytab, false);
    }

    private static String getKrb5LoginModuleName() {
      return (IBM_JAVA ? "com.ibm.security.auth.module.Krb5LoginModule"
          : "com.sun.security.auth.module.Krb5LoginModule");
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<>();

      if (IBM_JAVA) {
        // IBM JAVA's UseKeytab covers both keyTab and useKeyTab options
        options.put("useKeytab", keytab.startsWith("file://") ? keytab : "file://" + keytab);

        options.put("principal", principal);
        options.put("refreshKrb5Config", "true");

        // Both "initiator" and "acceptor"
        options.put("credsType", "both");
      } else {
        options.put("keyTab", keytab);
        options.put("principal", principal);
        options.put("useKeyTab", "true");
        options.put("storeKey", "true");
        options.put("doNotPrompt", "true");
        options.put("useTicketCache", "true");
        options.put("renewTGT", "true");
        options.put("refreshKrb5Config", "true");
        options.put("isInitiator", Boolean.toString(isInitiator));
      }

      String ticketCache = System.getenv(KRBCNAME);
      if (IBM_JAVA) {
        // If cache is specified via env variable, it takes priority
        if (ticketCache != null) {
          // IBM JAVA only respects system property so copy ticket cache to system property
          // The first value searched when "useDefaultCcache" is true.
          System.setProperty(KRBCNAME, ticketCache);
        } else {
          ticketCache = System.getProperty(KRBCNAME);
        }

        if (ticketCache != null) {
          options.put("useDefaultCcache", "true");
          options.put("renewTGT", "true");
        }
      } else {
        if (ticketCache != null) {
          options.put("ticketCache", ticketCache);
        }
      }
      options.put("debug", "true");

      return new AppConfigurationEntry[] {
          new AppConfigurationEntry(getKrb5LoginModuleName(),
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              options)};
    }
  }
}
