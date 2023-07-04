package io.confluent.connect.jdbc.select;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.Version;
import org.apache.kafka.connect.select.SelectTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class JdbcSelectTask extends SelectTask {
  public static final Logger log = LoggerFactory.getLogger(JdbcSelectTask.class);

  DatabaseDialect dialect;
  JdbcSelectConfig config;
  CachedConnectionProvider cachedConnectionProvider;

  private FieldsMetadata fieldsMetadata;

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
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    log.debug("Received {} records. First record kafka coordinates:({}-{}-{}).",
              recordsCount,
              first.topic(),
              first.kafkaPartition(),
              first.kafkaOffset()
    );
  }

  @Override
  public void stop() {

  }

  @Override
  public String version() {
    return Version.getVersion();
  }

}
