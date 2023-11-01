package io.confluent.connect.jdbc.select;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.DatabaseDialectRecommender;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.transforms.select.SelectTransformConfig;

import java.util.Map;

public class JdbcSelectTransformConfig extends SelectTransformConfig {

  public static final String CONNECTION_URL = JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG;
  private static final String CONNECTION_URL_DOC =
    "JDBC connection URL.\n"
      + "For example: ``jdbc:oracle:thin:@localhost:1521:orclpdb1``, "
      + "``jdbc:mysql://localhost/db_name``, "
      + "``jdbc:sqlserver://localhost;instance=SQLEXPRESS;"
      + "databaseName=db_name``";
  private static final String CONNECTION_URL_DISPLAY = "JDBC URL";

  public static final String CONNECTION_USER = JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG;
  private static final String CONNECTION_USER_DOC = "JDBC connection user.";
  private static final String CONNECTION_USER_DISPLAY = "JDBC User";

  public static final String CONNECTION_PASSWORD =
    JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG;
  private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
  private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

  private static final String CONNECTION_GROUP = "Connection";
  private static final String DATA_MAPPING_GROUP = "DataMapping";

  public static final ConfigDef CONFIG_DEF = org.apache.kafka.connect.transforms.select.SelectTransformConfig.CONFIG_DEF
    // Connection
    .define(
      CONNECTION_URL,
      ConfigDef.Type.STRING,
      ConfigDef.NO_DEFAULT_VALUE,
      ConfigDef.Importance.HIGH,
      CONNECTION_URL_DOC,
      CONNECTION_GROUP,
      1,
      ConfigDef.Width.LONG,
      CONNECTION_URL_DISPLAY
    )
    .define(
      CONNECTION_USER,
      ConfigDef.Type.STRING,
      null,
      ConfigDef.Importance.HIGH,
      CONNECTION_USER_DOC,
      CONNECTION_GROUP,
      2,
      ConfigDef.Width.MEDIUM,
      CONNECTION_USER_DISPLAY
    )
    .define(
      CONNECTION_PASSWORD,
      ConfigDef.Type.PASSWORD,
      null,
      ConfigDef.Importance.HIGH,
      CONNECTION_PASSWORD_DOC,
      CONNECTION_GROUP,
      3,
      ConfigDef.Width.MEDIUM,
      CONNECTION_PASSWORD_DISPLAY
    );


  public final String connectionUrl;
  public final String connectionUser;
  public final String connectionPassword;

  public JdbcSelectTransformConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
    connectionUrl = getString(CONNECTION_URL);
    connectionUser = getString(CONNECTION_USER);
    connectionPassword = getPasswordValue(CONNECTION_PASSWORD);
  }

  private String getPasswordValue(String key) {
    Password password = getPassword(key);
    if (password != null) {
      return password.value();
    }
    return null;
  }


}
