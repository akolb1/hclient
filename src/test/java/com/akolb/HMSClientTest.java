package com.akolb;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.akolb.Util.getServerUri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HMSClientTest {
  private static final String PARAM_NAME = "param";
  private static final String VALUE_NAME = "value";
  private static final String TEST_DATABASE="hmsclienttest";
  private static final String TEST_DATABASE_DESCRIPTION="hmsclienttest description";
  private static final ImmutableMap<String, String> TEST_DATABASE_PARAMS =
      new ImmutableMap.Builder<String, String>()
      .put(PARAM_NAME, VALUE_NAME)
      .build();

  private static final String TEST_TABLE_NAME="test1";
  private static final Table TEST_TABLE =
      Util.TableBuilder.buildDefaultTable(TEST_DATABASE, TEST_TABLE_NAME);

  private static HMSClient client;

  @BeforeAll
  static void init() throws Exception {
    // Create client and default test database
    client =
        new HMSClient(getServerUri(null, null), null);
    client.createDatabase(TEST_DATABASE, TEST_DATABASE_DESCRIPTION, null,
        TEST_DATABASE_PARAMS);
  }

  @AfterAll
  static void shutdown() throws TException {
    // Destroy test database
    client.dropDatabase(TEST_DATABASE);
  }

  /**
   * Verify that list of databases contains "default" and test database
   * @throws Exception
   */
  @Test
  void getAllDatabases() throws Exception {
    Set<String> databases = client.getAllDatabases(null);
    assertThat(databases, hasItem("default"));
    assertThat(databases, hasItem(TEST_DATABASE));
    assertThat(client.getAllDatabases(TEST_DATABASE), contains(TEST_DATABASE));
  }

  /**
   * Verify that an attempt to create an existing database throws AlreadyExistsException.
   */
  @Test
  void createExistingDatabase() {
    Throwable exception = assertThrows(AlreadyExistsException.class,
        () -> client.createDatabase(TEST_DATABASE));
  }

  /**
   * Verify that getDatabase() returns all expected fields
   * @throws TException if fails to get database info
   */
  @Test
  void getDatabase() throws TException {
    Database db = client.getDatabase(TEST_DATABASE);
    assertThat(db.getName(), equalToIgnoringCase(TEST_DATABASE));
    assertThat(db.getDescription(), equalTo(TEST_DATABASE_DESCRIPTION));
    assertThat(db.getParameters(), equalTo(TEST_DATABASE_PARAMS));
    assertThat(db.getLocationUri(), containsString(TEST_DATABASE));
  }

  @Test
  void getAllTables() throws TException {
    try {
      client.createTable(TEST_TABLE);
      assertThat(client.getAllTables(TEST_DATABASE, null), contains(TEST_TABLE_NAME));
    } finally {
      client.dropTable(TEST_DATABASE, TEST_TABLE_NAME);
    }
  }

}