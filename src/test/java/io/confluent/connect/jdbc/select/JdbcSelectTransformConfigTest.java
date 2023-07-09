package io.confluent.connect.jdbc.select;

import io.confluent.connect.jdbc.transforms.JdbcSelectTransformConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class JdbcSelectTransformConfigTest {

  private Map<String, String> props = new HashMap<>();
  private JdbcSelectTransformConfig config;

  @BeforeEach
  public void beforeEach() {
    // add the minimum settings only
    props.put("connection.url", "jdbc:mysql://something"); // we won't connect
  }

  @AfterEach
  public void afterEach() {
    props.clear();
    config = null;
  }

  @Test
  public void shouldFailToCreateConfigWithoutConnectionUrl() {
    Assertions.assertThrows(ConfigException.class, () -> {
      props.remove(JdbcSelectTransformConfig.CONNECTION_URL);
      createConfig();
    });
  }

  @Test
  public void shouldFailToCreateConfigWithEmptyTableNameFormat() {
    Assertions.assertThrows(ConfigException.class, () -> {
      props.put(JdbcSelectTransformConfig.TABLE_NAME_FORMAT, "");
      createConfig();
    });
  }

  protected void createConfig() {
    config = new JdbcSelectTransformConfig(props);
  }

}
