package io.confluent.connect.jdbc.select;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.Version;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.select.SelectTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcSelectTask extends SelectTask {
  public static final Logger log = LoggerFactory.getLogger(JdbcSelectTask.class);

  DatabaseDialect dialect;
  JdbcSelectConfig config;
  CachedConnectionProvider cachedConnectionProvider;
  Connection connection;

  private final Map<TableId, JdbcRecordSelector> selectorByTable = new HashMap<>();

  @Override
  public void start(Map<String, String> props) {
    super.start(props);
    log.info("Starting JDBC Select task");
    config = new JdbcSelectConfig(props);
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
                                List<String> fields,
                                SchemaAndValue keys)
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
  public void stop() {
    log.info("Stopping select task");
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

  @Override
  public String version() {
    return Version.getVersion();
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
