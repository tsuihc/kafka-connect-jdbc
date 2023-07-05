package io.confluent.connect.jdbc.select;

import com.github.dockerjava.api.model.HostConfig;
import org.junit.BeforeClass;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;

@Testcontainers
public class JdbcSelectTaskIT {

  private static final String USERNAME = "test";
  private static final String PASSWORD = "test";

  @SuppressWarnings("resource")
  @Container
  private static final MySQLContainer<?> container = new MySQLContainer<>("mysql:5.7")
    .withDatabaseName("test")
    .withUsername(USERNAME)
    .withPassword(PASSWORD);

  private static Connection connection;

  @BeforeClass
  public static void init() {
    System.setProperty("DOCKER_HOST", "tcp://10.16.2.103:2375");
  }

  @BeforeAll
  static void setup()
  throws SQLException {
    String jdbcUrl = container.getJdbcUrl();
    String username = container.getUsername();
    String password = container.getPassword();

    connection = DriverManager.getConnection(jdbcUrl, username, password);
    // 创建test表
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(255))");
      statement.executeUpdate("INSERT INTO test VALUES (1, 'Test Data')");
    }
  }

  @Test
  void testDataRetrieval()
  throws SQLException {
    // 执行数据检索
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery("SELECT * FROM test")) {
      // 断言数据正确性
      if (resultSet.next()) {
        int id = resultSet.getInt("id");
        String name = resultSet.getString("name");
        Assertions.assertEquals(1, id);
        Assertions.assertEquals("Test Data", name);
      }
    }
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
