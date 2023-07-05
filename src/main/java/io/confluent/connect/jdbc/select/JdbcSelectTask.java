package io.confluent.connect.jdbc.select;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.Version;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.select.SelectTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class JdbcSelectTask extends SelectTask {
  public static final Logger log = LoggerFactory.getLogger(JdbcSelectTask.class);

  DatabaseDialect dialect;
  JdbcSelectConfig config;
  CachedConnectionProvider cachedConnectionProvider;

  private Schema keySchema;
  private PreparedStatement selectStatement;

  @Override
  public void start(Map<String, String> props) {
    log.info("Starting JDBC Select task");
    config = new JdbcSelectConfig(props);
    if (config.dialectName != null && !config.dialectName.trim().isEmpty()) {
      dialect = DatabaseDialects.create(config.dialectName, config);
    } else {
      dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
    }
  }

  @Override
  protected SourceRecord select(SchemaAndValue keys)
  throws Exception {
    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, keys.schema())) {
      keySchema = keys.schema();
      schemaChanged = true;
    }
    if (schemaChanged || selectStatement == null) {
      // re-initialize everything
      dialect.buildSelectStatement()
    }


    return null;
  }

  @Override
  public void stop() {
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

}
