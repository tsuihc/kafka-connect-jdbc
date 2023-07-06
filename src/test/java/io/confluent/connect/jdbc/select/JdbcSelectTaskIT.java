package io.confluent.connect.jdbc.select;

import com.github.dockerjava.api.model.HostConfig;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.select.SelectTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.BeforeClass;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Testcontainers
public class JdbcSelectTaskIT {

  @SuppressWarnings("resource")
  @Container
  private static final MySQLContainer<?> container = new MySQLContainer<>("mysql:5.7")
    .withDatabaseName("test")
    .withUsername("test")
    .withPassword("test");

  private static Connection connection;

  @BeforeAll
  public static void setup()
  throws SQLException {
    String jdbcUrl = container.getJdbcUrl();
    String username = container.getUsername();
    String password = container.getPassword();

    connection = DriverManager.getConnection(jdbcUrl, username, password);
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(255))");
      statement.executeUpdate("INSERT INTO test VALUES (1, 'Test Data')");
    }
  }

  @Test
  public void shouldSelect() {
    String destination = "test.test";
    Schema keySchema = new SchemaBuilder(Schema.Type.INT32).name("id").build();
    int keyValue = 1;
    SinkRecord record = new SinkRecord(destination, 0, keySchema, keyValue, null, null, 0);

    Map<String, String> props = new HashMap<>();
    props.put(JdbcSelectConfig.CONNECTION_URL, container.getJdbcUrl());
    props.put(JdbcSelectConfig.CONNECTION_USER, container.getUsername());
    props.put(JdbcSelectConfig.CONNECTION_PASSWORD, container.getPassword());
    props.put(JdbcSelectConfig.DESTINATIONS, destination);

    SelectTask task = new JdbcSelectTask();
    task.start(props);
    task.put(Collections.singleton(record));
  }

  @AfterAll
  static void cleanup()
  throws SQLException {
    // 删除test表
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate("DROP TABLE test");
    }

    connection.close();
  }

}
