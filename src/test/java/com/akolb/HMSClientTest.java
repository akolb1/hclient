package com.akolb;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.akolb.Util.getServerUri;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HMSClientTest {
  private static String TEST_DATABASE="hmsclienttest";
  private static HMSClient client;

  @BeforeAll
  static void init() throws Exception {
    client =
        new HMSClient(getServerUri(null, null), null);
    client.createDatabase(TEST_DATABASE);
  }

  @AfterAll
  static void shutdown() throws TException {
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
  }

  @Test
  void getAllTables() {
  }

  /**
   * Verify that an attempt to create an existing database throws AlreadyExistsException.
   */
  @Test
  void createExistingDatabase() {
    Throwable exception = assertThrows(AlreadyExistsException.class,
        () -> client.createDatabase(TEST_DATABASE));
  }
}