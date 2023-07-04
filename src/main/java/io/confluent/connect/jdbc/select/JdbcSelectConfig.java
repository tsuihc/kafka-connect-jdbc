package io.confluent.connect.jdbc.select;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import java.time.ZoneId;
import java.util.*;

public class JdbcSelectConfig extends JdbcSinkConfig {

//  public enum PrimaryKeyMode {
//    NONE,
//    KAFKA,
//    RECORD_KEY,
//    RECORD_VALUE;
//  }
//
//  public static final List<String> DEFAULT_KAFKA_PK_NAMES = List.of(
//      "__connect_topic",
//      "__connect_partition",
//      "__connect_offset"
//  );
//
//  public static final String CONNECTION_URL = JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG;
//  private static final String CONNECTION_URL_DOC =
//      "JDBC connection URL.\n"
//          + "For example: ``jdbc:oracle:thin:@localhost:1521:orclpdb1``, "
//          + "``jdbc:mysql://localhost/db_name``, "
//          + "``jdbc:sqlserver://localhost;instance=SQLEXPRESS;"
//          + "databaseName=db_name``";
//  private static final String CONNECTION_URL_DISPLAY = "JDBC URL";
//
//  public static final String CONNECTION_USER = JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG;
//  private static final String CONNECTION_USER_DOC = "JDBC connection user.";
//  private static final String CONNECTION_USER_DISPLAY = "JDBC User";
//
//  public static final String CONNECTION_PASSWORD =
//      JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG;
//  private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
//  private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";
//
//  public static final String CONNECTION_ATTEMPTS =
//      JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG;
//  private static final String CONNECTION_ATTEMPTS_DOC =
//      JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_DOC;
//  private static final String CONNECTION_ATTEMPTS_DISPLAY =
//      JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_DISPLAY;
//  public static final int CONNECTION_ATTEMPTS_DEFAULT =
//      JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_DEFAULT;
//
//  public static final String CONNECTION_BACKOFF =
//      JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG;
//  private static final String CONNECTION_BACKOFF_DOC =
//      JdbcSourceConnectorConfig.CONNECTION_BACKOFF_DOC;
//  private static final String CONNECTION_BACKOFF_DISPLAY =
//      JdbcSourceConnectorConfig.CONNECTION_BACKOFF_DISPLAY;
//  public static final long CONNECTION_BACKOFF_DEFAULT =
//      JdbcSourceConnectorConfig.CONNECTION_BACKOFF_DEFAULT;
//
//  public static final String DIALECT_NAME_CONFIG = "dialect.name";
//  private static final String DIALECT_NAME_DISPLAY = "Database Dialect";
//  public static final String DIALECT_NAME_DEFAULT = "";
//  private static final String DIALECT_NAME_DOC =
//      "The name of the database dialect that should be used for this connector. By default this "
//          + "is empty, and the connector automatically determines the dialect based upon the "
//          + "JDBC connection URL. Use this if you want to override that behavior and use a "
//          + "specific dialect. All properly-packaged dialects in the JDBC connector plugin "
//          + "can be used.";
//
//  public static final String DB_TIMEZONE_CONFIG = "db.timezone";
//  public static final String DB_TIMEZONE_DEFAULT = "UTC";
//  private static final String DB_TIMEZONE_CONFIG_DOC =
//      "Name of the JDBC timezone that should be used in the connector when "
//          + "inserting time-based values. Defaults to UTC.";
//  private static final String DB_TIMEZONE_CONFIG_DISPLAY = "DB Time Zone";
//
//  public static final String PK_FIELDS = "pk.fields";
//  private static final String PK_FIELDS_DEFAULT = "";
//  private static final String PK_FIELDS_DOC =
//      "List of comma-separated primary key field names. The runtime interpretation of this config"
//          + " depends on the ``pk.mode``:\n"
//          + "``none``\n"
//          + "    Ignored as no fields are used as primary key in this mode.\n"
//          + "``kafka``\n"
//          + "    Must be a trio representing the Kafka coordinates, defaults to ``"
//          + StringUtils.join(DEFAULT_KAFKA_PK_NAMES, ",") + "`` if empty.\n"
//          + "``record_key``\n"
//          + "    If empty, all fields from the key struct will be used, otherwise used to extract the"
//          + " desired fields - for primitive key only a single field name must be configured.\n"
//          + "``record_value``\n"
//          + "    If empty, all fields from the value struct will be used, otherwise used to extract "
//          + "the desired fields.";
//  private static final String PK_FIELDS_DISPLAY = "Primary Key Fields";
//
//  public static final String PK_MODE = "pk.mode";
//  private static final String PK_MODE_DEFAULT = "none";
//  private static final String PK_MODE_DOC =
//      "The primary key mode, also refer to ``" + PK_FIELDS + "`` documentation for interplay. "
//          + "Supported modes are:\n"
//          + "``none``\n"
//          + "    No keys utilized.\n"
//          + "``kafka``\n"
//          + "    Kafka coordinates are used as the PK.\n"
//          + "``record_key``\n"
//          + "    Field(s) from the record key are used, which may be a primitive or a struct.\n"
//          + "``record_value``\n"
//          + "    Field(s) from the record value are used, which must be a struct.";
//  private static final String PK_MODE_DISPLAY = "Primary Key Mode";
//
//  public static final String FIELDS_WHITELIST = "fields.whitelist";
//  private static final String FIELDS_WHITELIST_DEFAULT = "";
//  private static final String FIELDS_WHITELIST_DOC =
//      "List of comma-separated record value field names. If empty, all fields from the record "
//          + "value are utilized, otherwise used to filter to the desired fields.\n"
//          + "Note that ``" + PK_FIELDS + "`` is applied independently in the context of which field"
//          + "(s) form the primary key columns in the destination database,"
//          + " while this configuration is applicable for the other columns.";
//  private static final String FIELDS_WHITELIST_DISPLAY = "Fields Whitelist";
//
//  private static final String CONNECTION_GROUP = "Connection";
//  private static final String DATA_MAPPING_GROUP = "Data Mapping";

//  public static final ConfigDef CONFIG_DEF = new ConfigDef()
//      // Connection
//      .define(
//          CONNECTION_URL,
//          ConfigDef.Type.STRING,
//          ConfigDef.NO_DEFAULT_VALUE,
//          ConfigDef.Importance.HIGH,
//          CONNECTION_URL_DOC,
//          CONNECTION_GROUP,
//          1,
//          ConfigDef.Width.LONG,
//          CONNECTION_URL_DISPLAY
//      )
//      .define(
//          CONNECTION_USER,
//          ConfigDef.Type.STRING,
//          null,
//          ConfigDef.Importance.HIGH,
//          CONNECTION_USER_DOC,
//          CONNECTION_GROUP,
//          2,
//          ConfigDef.Width.MEDIUM,
//          CONNECTION_USER_DISPLAY
//      )
//      .define(
//          CONNECTION_PASSWORD,
//          ConfigDef.Type.PASSWORD,
//          null,
//          ConfigDef.Importance.HIGH,
//          CONNECTION_PASSWORD_DOC,
//          CONNECTION_GROUP,
//          3,
//          ConfigDef.Width.MEDIUM,
//          CONNECTION_PASSWORD_DISPLAY
//      )
//      .define(
//          DIALECT_NAME_CONFIG,
//          ConfigDef.Type.STRING,
//          DIALECT_NAME_DEFAULT,
//          DatabaseDialectRecommender.INSTANCE,
//          ConfigDef.Importance.LOW,
//          DIALECT_NAME_DOC,
//          CONNECTION_GROUP,
//          4,
//          ConfigDef.Width.LONG,
//          DIALECT_NAME_DISPLAY,
//          DatabaseDialectRecommender.INSTANCE
//      )
//      .define(
//          CONNECTION_ATTEMPTS,
//          ConfigDef.Type.INT,
//          CONNECTION_ATTEMPTS_DEFAULT,
//          ConfigDef.Range.atLeast(1),
//          ConfigDef.Importance.LOW,
//          CONNECTION_ATTEMPTS_DOC,
//          CONNECTION_GROUP,
//          5,
//          ConfigDef.Width.SHORT,
//          CONNECTION_ATTEMPTS_DISPLAY
//      )
//      .define(
//          CONNECTION_BACKOFF,
//          ConfigDef.Type.LONG,
//          CONNECTION_BACKOFF_DEFAULT,
//          ConfigDef.Importance.LOW,
//          CONNECTION_BACKOFF_DOC,
//          CONNECTION_GROUP,
//          6,
//          ConfigDef.Width.SHORT,
//          CONNECTION_BACKOFF_DISPLAY
//      )
//      .define(
//          PK_MODE,
//          ConfigDef.Type.STRING,
//          PK_MODE_DEFAULT,
//          ConfigDef.Importance.HIGH,
//          PK_MODE_DOC,
//          DATA_MAPPING_GROUP,
//          2,
//          ConfigDef.Width.MEDIUM,
//          PK_MODE_DISPLAY,
//          PrimaryKeyModeRecommender.INSTANCE
//      )
//      .define(
//          PK_FIELDS,
//          ConfigDef.Type.LIST,
//          PK_FIELDS_DEFAULT,
//          ConfigDef.Importance.MEDIUM,
//          PK_FIELDS_DOC,
//          DATA_MAPPING_GROUP,
//          3,
//          ConfigDef.Width.LONG, PK_FIELDS_DISPLAY
//      )
//      .define(
//          FIELDS_WHITELIST,
//          ConfigDef.Type.LIST,
//          FIELDS_WHITELIST_DEFAULT,
//          ConfigDef.Importance.MEDIUM,
//          FIELDS_WHITELIST_DOC,
//          DATA_MAPPING_GROUP,
//          4,
//          ConfigDef.Width.LONG,
//          FIELDS_WHITELIST_DISPLAY
//      )
//      .define(
//          DB_TIMEZONE_CONFIG,
//          ConfigDef.Type.STRING,
//          DB_TIMEZONE_DEFAULT,
//          TimeZoneValidator.INSTANCE,
//          ConfigDef.Importance.MEDIUM,
//          DB_TIMEZONE_CONFIG_DOC,
//          DATA_MAPPING_GROUP,
//          5,
//          ConfigDef.Width.MEDIUM,
//          DB_TIMEZONE_CONFIG_DISPLAY
//      );


//  public final String connectorName;
//  public final String connectionUrl;
//  public final String connectionUser;
//  public final String connectionPassword;
//  public final PrimaryKeyMode pkMode;
//  public final List<String> pkFields;
//  public final Set<String> fieldsWhitelist;
//  public final String dialectName;
//  public final TimeZone timeZone;

  public JdbcSelectConfig(Map<?, ?> props) {
    super(props);
//    connectorName = ConfigUtils.connectorName(props);
//    connectionUrl = getString(CONNECTION_URL);
//    connectionUser = getString(CONNECTION_USER);
//    connectionPassword = getPasswordValue(CONNECTION_PASSWORD);
//    pkMode = PrimaryKeyMode.valueOf(getString(PK_MODE).toUpperCase());
//    pkFields = getList(PK_FIELDS);
//    fieldsWhitelist = new HashSet<>(getList(FIELDS_WHITELIST));
//    String dbTimeZone = getString(DB_TIMEZONE_CONFIG);
//    dialectName = getString(DIALECT_NAME_CONFIG);
//    timeZone = TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
  }


}
