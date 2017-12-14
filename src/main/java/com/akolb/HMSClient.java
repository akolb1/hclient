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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.akolb.Util.makePartition;

final class HMSClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(HMSClient.class);
  private static final String METASTORE_URI = "hive.metastore.uris";
  private static final String CONFIG_DIR = "/etc/hive/conf";
  private static final String HIVE_SITE = "hive-site.xml";
  private static final String CORE_SITE = "core-site.xml";
  private static final String PRINCIPAL_KEY = "hive.metastore.kerberos.principal";
  private static final long SOCKET_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(600);

  private final String confDir;
  private ThriftHiveMetastore.Iface client;
  private TTransport transport;
  private URI serverURI;

  @Override
  public String toString() {
    return serverURI.toString();
  }

  HMSClient(@Nullable URI uri)
      throws TException, IOException, InterruptedException, LoginException, URISyntaxException {
    this(uri, CONFIG_DIR);
  }

  HMSClient(@Nullable URI uri, @Nullable String confDir)
      throws TException, IOException, InterruptedException, LoginException, URISyntaxException {
    this.confDir = (confDir == null ? CONFIG_DIR : confDir);
    getClient(uri);
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
   * @throws MetaException        if fails to login using kerberos credentials
   * @throws IOException          if fails connecting to metastore
   * @throws InterruptedException if interrupted during kerberos setup
   */
  private void getClient(@Nullable URI uri)
      throws TException, IOException, InterruptedException, URISyntaxException, LoginException {
    HiveConf conf = new HiveConf();
    addResource(conf, HIVE_SITE);
    if (uri != null) {
      conf.set(METASTORE_URI, uri.toString());
    }

    // Pick up the first URI from the list of available URIs
    serverURI = uri != null ?
        uri :
        new URI(conf.get(METASTORE_URI).split(",")[0]);

    String principal = conf.get(PRINCIPAL_KEY);

    if (principal == null) {
      open(conf, serverURI);
      return;
    }

    LOG.debug("Opening kerberos connection to HMS");
    addResource(conf, CORE_SITE);

    Configuration hadoopConf = new Configuration();
    addResource(hadoopConf, HIVE_SITE);
    addResource(hadoopConf, CORE_SITE);

    // Kerberos magic
    UserGroupInformation.setConfiguration(hadoopConf);
    UserGroupInformation.getLoginUser()
        .doAs((PrivilegedExceptionAction<TTransport>)
            () -> open(conf, serverURI));
  }

  boolean dbExists(@NotNull String dbName) throws TException {
    return getAllDatabases(dbName).contains(dbName);
  }

  boolean tableExists(@NotNull String dbName, @NotNull String tableName) throws TException {
    return getAllTables(dbName, tableName).contains(tableName);
  }

  /**
   * Return all databases with name matching the filter
   *
   * @param filter Regexp. Can be null or empty in which case everything matches
   * @return list of database names matching the filter
   * @throws MetaException
   */
  Set<String> getAllDatabases(@Nullable String filter) throws TException {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.get_all_databases());
    }
    return client.get_all_databases()
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  List<String> getAllDatabasesNoException() {
    try {
      return client.get_all_databases();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  Set<String> getAllTables(@NotNull String dbName, @Nullable String filter) throws TException {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.get_all_tables(dbName));
    }
    return client.get_all_tables(dbName)
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  List<String> getAllTablesNoException(@NotNull String dbName) {
    try {
      return client.get_all_tables(dbName);
    } catch (TException e) {
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
    client.create_database(db);
  }

  void createTable(Table table) throws TException {
    client.create_table(table);
  }

  /**
   * Create tabe but convert any exception to unchecked {@link RuntimeException}
   *
   * @param table table to create
   */
  void createTableNoException(@NotNull Table table) {
    try {
      createTable(table);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  void dropTable(@NotNull String dbName, @NotNull String tableName) throws TException {
    client.drop_table(dbName, tableName, true);
  }

  /**
   * Drop table but convert any exception to unchecked {@link RuntimeException}.
   *
   * @param dbName    Database name
   * @param tableName Table name
   */
  void dropTableNoException(@NotNull String dbName, @NotNull String tableName) {
    try {
      dropTable(dbName, tableName);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  Table getTable(@NotNull String dbName, @NotNull String tableName) throws TException {
    return client.get_table(dbName, tableName);
  }

  Table getTableNoException(@NotNull String dbName, @NotNull String tableName) {
    try {
      return getTable(dbName, tableName);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  void createPartition(@NotNull Table table, @NotNull List<String> values) throws TException {
    client.add_partition(makePartition(table, values));
  }

  void createPartitions(List<Partition> partitions) throws TException {
    client.add_partitions(partitions);
  }

  void createPartitionNoException(@NotNull Partition partition) {
    try {
      client.add_partition(partition);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  List<Partition> listPartitions(@NotNull String dbName,
                                 @NotNull String tableName) throws TException {
    return client.get_partitions(dbName, tableName, (short) -1);
  }

  List<Partition> listPartitionsNoException(@NotNull String dbName, @NotNull String tableName) {
    try {
      return listPartitions(dbName, tableName);
    } catch (TException e) {
      LOG.error("Failed to list partitions", e);
      throw new RuntimeException(e);
    }
  }

  long getCurrentNotificationId() throws TException {
    return client.get_current_notificationEventId().getEventId();
  }

  long getCurrentNotificationIdNoException() {
    try {
      return getCurrentNotificationId();
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  List<String> getPartitionNames(@NotNull String dbName,
                                 @NotNull String tableName) throws TException {
    return client.get_partition_names(dbName, tableName, (short) -1);
  }

  List<String> getPartitionNamesNoException(@NotNull String dbName, @NotNull String tableName) {
    try {
      return getPartitionNames(dbName, tableName);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean dropPartition(@NotNull String dbName, @NotNull String tableName,
                               @NotNull List<String> arguments)
      throws TException {
    return client.drop_partition(dbName, tableName, arguments, true);
  }

  public boolean dropPartitionNoException(@NotNull String dbName, @NotNull String tableName,
                                          @NotNull List<String> arguments) {
    try {
      return dropPartition(dbName, tableName, arguments);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  List<Partition> getPartitions(@NotNull String dbName, @NotNull String tableName) throws TException {
    return client.get_partitions(dbName, tableName, (short)-1);
  }

  List<Partition> getPartitionsNoException(@NotNull String dbName, @NotNull String tableName) {
    try {
      return getPartitions(dbName, tableName);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  DropPartitionsResult dropPartitions(@NotNull String dbName, @NotNull String tableName,
                                      @Nullable List<String> partNames) throws TException {
    if (partNames == null) {
      return dropPartitions(dbName, tableName, getPartitionNames(dbName, tableName));
    }
    if (partNames.isEmpty()) {
      return null;
    }
    return client.drop_partitions_req(new DropPartitionsRequest(dbName,
        tableName, RequestPartsSpec.names(partNames)));
  }

  void dropPartitionsNoException(@NotNull String dbName, @NotNull String tableName,
                                 @Nullable List<String> names) {
    try {
      dropPartitions(dbName, tableName, names);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  List<Partition> getPartitionsByNames(@NotNull String dbName, @NotNull String tableName,
                                       @Nullable List<String>names) throws TException {
    if (names == null) {
      return client.get_partitions_by_names(dbName, tableName,
          getPartitionNames(dbName, tableName));
    }
    return client.get_partitions_by_names(dbName, tableName, names);
  }

  List<Partition> getPartitionsByNamesNoException(@NotNull String dbName, @NotNull String tableName,
                                                  @Nullable List<String>names) {
    try {
      return getPartitionsByNames(dbName, tableName, names);
    } catch (TException e) {
      throw  new RuntimeException(e);
    }
  }

  private TTransport open(HiveConf conf, @NotNull URI uri) throws
      TException, IOException, LoginException {
    boolean useSasl = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL);
    boolean useFramedTransport = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT);
    boolean useCompactProtocol = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL);
    LOG.debug("Connecting to {}, framedTransport = {}", uri, useFramedTransport);

    transport = new TSocket(uri.getHost(), uri.getPort(), (int) SOCKET_TIMEOUT_MS);

    if (useSasl) {
      LOG.debug("Using SASL authentication");
      HadoopThriftAuthBridge.Client authBridge =
          ShimLoader.getHadoopThriftAuthBridge().createClient();
      // check if we should use delegation tokens to authenticate
      // the call below gets hold of the tokens if they are set up by hadoop
      // this should happen on the map/reduce tasks if the client added the
      // tokens into hadoop's credential store in the front end during job
      // submission.
      String tokenSig = conf.get("hive.metastore.token.signature");
      // tokenSig could be null
      String tokenStrForm = Utils.getTokenStrForm(tokenSig);
      if (tokenStrForm != null) {
        LOG.debug("Using delegation tokens");
        // authenticate using delegation tokens via the "DIGEST" mechanism
        transport = authBridge.createClientTransport(null, uri.getHost(),
            "DIGEST", tokenStrForm, transport,
            MetaStoreUtils.getMetaStoreSaslProperties(conf));
      } else {
        LOG.debug("Using principal");
        String principalConfig =
            conf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL);
        LOG.debug("Using principal {}", principalConfig);
        transport = authBridge.createClientTransport(
            principalConfig, uri.getHost(), "KERBEROS", null,
            transport, MetaStoreUtils.getMetaStoreSaslProperties(conf));
      }
    }

    transport = useFramedTransport ? new TFastFramedTransport(transport) : transport;
    TProtocol protocol = useCompactProtocol ?
        new TCompactProtocol(transport) :
        new TBinaryProtocol(transport);
    client = new ThriftHiveMetastore.Client(protocol);
    transport.open();
    if (!useSasl && conf.getBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI)) {
      UserGroupInformation ugi = Utils.getUGI();
      client.set_ugi(ugi.getUserName(), Arrays.asList(ugi.getGroupNames()));
    }
    LOG.debug("Connected to metastore, using compact protocol = {}", useCompactProtocol);
    return transport;
  }

  @Override
  public void close() throws Exception {
    if ((transport != null) && transport.isOpen()) {
      LOG.debug("Closing thrift transport");
      transport.close();
    }
  }
}
