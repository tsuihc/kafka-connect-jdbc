package io.confluent.connect.jdbc.transforms;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.select.JdbcRecordSelector;
import io.confluent.connect.jdbc.select.JdbcSelectConfig;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.select.SelectTransform;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcSelectTransform extends SelectTransform {

  DatabaseDialect dialect;
  JdbcSelectTransformConfig config;
  CachedConnectionProvider cachedConnectionProvider;
  Connection connection;

  private final Map<TableId, JdbcRecordSelector> selectorByTable = new HashMap<>();

  @Override
  public ConfigDef config() {
    return JdbcSelectConfig.CONFIG_DEF;
  }

  @Override
  public void configure(Map<String, ?> props) {
    super.configure(props);
    log.info("Configuring JDBC Select transform");
    config = new JdbcSelectTransformConfig(props);
    if (config.dialectName != null && !config.dialectName.trim().isEmpty()) {
      dialect = DatabaseDialects.create(config.dialectName, config);
    } else {
      dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
    }
    cachedConnectionProvider = new CachedConnectionProvider(dialect, 3, 1000L);
    connection = cachedConnectionProvider.getConnection();
  }

  @Override
  protected SourceRecord select(String destination,
                                SchemaAndValue keys,
                                List<String> fields)
  throws Exception {
    TableId tableId = destinationTable(destination);
    JdbcRecordSelector selector = selectorByTable.get(tableId);
    if (selector == null) {
      selector = new JdbcRecordSelector(tableId, fields, dialect, connection);
      selectorByTable.put(tableId, selector);
    }
    return selector.select(keys);
  }

  @Override
  public void close() {
    super.close();
    log.info("Closing JDBC Select transform");
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (Throwable t) {
      log.warn("Error while closing connection", t);
    } finally {
      connection = null;
    }

    try {
      if (cachedConnectionProvider != null) {
        cachedConnectionProvider.close();
      }
    } catch (Throwable t) {
      log.warn("Error while closing connection-provider", t);
    } finally {
      cachedConnectionProvider = null;
    }

    try {
      if (dialect != null) {
        dialect.close();
      }
    } catch (Throwable t) {
      log.warn("Error while closing the {} dialect", dialect.name(), t);
    } finally {
      dialect = null;
    }
  }

  TableId destinationTable(String topic) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
        "Destination table name for topic '%s' is empty using the format string '%s'",
        topic,
        config.tableNameFormat
      ));
    }
    return dialect.parseTableIdentifier(tableName);
  }

}
