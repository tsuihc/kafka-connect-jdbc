package io.confluent.connect.jdbc.select;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.select.SelectConfig;

import java.time.ZoneId;
import java.util.*;

public class JdbcSelectConfig extends SelectConfig {

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

  public static final String DIALECT_NAME_CONFIG = "dialect.name";
  private static final String DIALECT_NAME_DISPLAY = "Database Dialect";
  public static final String DIALECT_NAME_DEFAULT = "";
  private static final String DIALECT_NAME_DOC =
      "The name of the database dialect that should be used for this connector. By default this "
          + "is empty, and the connector automatically determines the dialect based upon the "
          + "JDBC connection URL. Use this if you want to override that behavior and use a "
          + "specific dialect. All properly-packaged dialects in the JDBC connector plugin "
          + "can be used.";

  private static final String CONNECTION_GROUP = "Connection";

  public static final ConfigDef CONFIG_DEF = SelectConfig.CONFIG_DEF
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
      )
      .define(
          DIALECT_NAME_CONFIG,
          ConfigDef.Type.STRING,
          DIALECT_NAME_DEFAULT,
          DatabaseDialectRecommender.INSTANCE,
          ConfigDef.Importance.LOW,
          DIALECT_NAME_DOC,
          CONNECTION_GROUP,
          4,
          ConfigDef.Width.LONG,
          DIALECT_NAME_DISPLAY,
          DatabaseDialectRecommender.INSTANCE
      );

  public final String connectionUrl;
  public final String connectionUser;
  public final String connectionPassword;
  public final String dialectName;

  public JdbcSelectConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
    connectionUrl = getString(CONNECTION_URL);
    connectionUser = getString(CONNECTION_USER);
    connectionPassword = getPasswordValue(CONNECTION_PASSWORD);
    dialectName = getString(DIALECT_NAME_CONFIG);
  }

  private String getPasswordValue(String key) {
    Password password = getPassword(key);
    if (password != null) {
      return password.value();
    }
    return null;
  }

}
