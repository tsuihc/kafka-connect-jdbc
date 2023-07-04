package io.confluent.connect.jdbc.select;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.TableAlterOrCreateException;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.source.SchemaMapping;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public class JdbcRecordSelector {
  private static final Logger log = LoggerFactory.getLogger(JdbcRecordSelector.class);

  private final TableId tableId;
  private final JdbcSelectConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private Schema keySchema;
  private Schema valueSchema;
  private SchemaPair schemaPair;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement selectStatement;
  private TableDefinition tabDef;

  private SchemaMapping schemaMapping;

  public JdbcRecordSelector(
      JdbcSelectConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection
  ) {
    this.tableId = tableId;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
  }

  public ConnectRecord<?> select(SinkRecord record)
  throws SQLException, TableAlterOrCreateException {
    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, record.keySchema())) {
      keySchema = record.keySchema();
      schemaChanged = true;
    }
    if (!Objects.equals(valueSchema, record.valueSchema())) {
      valueSchema = record.valueSchema();
      schemaChanged = true;
    }
    if (schemaChanged || selectStatement == null) {
      // re-initialize everything
      schemaPair = new SchemaPair(
          record.keySchema(),
          record.valueSchema()
      );
      fieldsMetadata = FieldsMetadata.extract(
          tableId.tableName(),
          config.pkMode,
          config.pkFields,
          config.fieldsWhitelist,
          schemaPair
      );
      dbStructure.createOrAmendIfNecessary(
          config,
          connection,
          tableId,
          fieldsMetadata
      );
      final String selectSql = getSelectSql();
      log.debug(
          "select sql: {}, meta: {}",
          selectSql,
          fieldsMetadata
      );
      close();
      selectStatement = dbDialect.createPreparedStatement(connection, selectSql);
      tabDef = dbStructure.tableDefinition(connection, tableId);
    }
    int index = 1;
    bindKeyFields(record, index);
    return executeSelect();
  }

  private String getSelectSql() {
    return dbDialect.buildSelectStatement(
        tableId,
        asColumns(fieldsMetadata.keyFieldNames),
        asColumns(fieldsMetadata.nonKeyFieldNames)
    );
  }

  private ConnectRecord<?> executeSelect()
  throws SQLException {
    ResultSet resultSet = selectStatement.executeQuery();
    if (!resultSet.next()) {
      return null;
    }
    if (schemaMapping == null) {
      String schemaName = tableId != null ? tableId.tableName() : null; // backwards compatible
      schemaMapping = SchemaMapping.create(schemaName, resultSet.getMetaData(), dbDialect);
    }
    Struct record = new Struct(schemaMapping.schema());
    for (SchemaMapping.FieldSetter setter : schemaMapping.fieldSetters()) {
      try {
        setter.setField(record, resultSet);
      } catch (IOException e) {
        log.warn("Error mapping fields into Connect record", e);
        throw new ConnectException(e);
      } catch (SQLException e) {
        log.warn("SQL error mapping fields into Connect record", e);
        throw new DataException(e);
      }
    }
    return new SourceRecord(null, null, null, record.schema(), record);
  }

  protected void bindKeyFields(SinkRecord record,
                               int index)
  throws SQLException {
    switch (config.pkMode) {
      case NONE:
        if (!fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new AssertionError();
        }
        break;

      case KAFKA:
        assert fieldsMetadata.keyFieldNames.size() == 3;
        bindField(index++, Schema.STRING_SCHEMA, record.topic(),
                  JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(0));
        bindField(index++, Schema.INT32_SCHEMA, record.kafkaPartition(),
                  JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(1));
        bindField(index++, Schema.INT64_SCHEMA, record.kafkaOffset(),
                  JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(2));
        break;

      case RECORD_KEY:
        if (schemaPair.keySchema.type().isPrimitive()) {
          assert fieldsMetadata.keyFieldNames.size() == 1;
          bindField(index++, schemaPair.keySchema, record.key(),
                    fieldsMetadata.keyFieldNames.iterator().next());
        } else {
          for (String fieldName : fieldsMetadata.keyFieldNames) {
            final Field field = schemaPair.keySchema.field(fieldName);
            bindField(index++, field.schema(), ((Struct) record.key()).get(field), fieldName);
          }
        }
        break;

      case RECORD_VALUE:
        for (String fieldName : fieldsMetadata.keyFieldNames) {
          final Field field = schemaPair.valueSchema.field(fieldName);
          bindField(index++, field.schema(), ((Struct) record.value()).get(field), fieldName);
        }
        break;

      default:
        throw new ConnectException("Unknown primary key mode: " + config.pkMode);
    }
  }

  protected void bindField(int index,
                           Schema schema,
                           Object value,
                           String fieldName)
  throws SQLException {
    ColumnDefinition colDef = tabDef == null ? null : tabDef.definitionForColumn(fieldName);
    dbDialect.bindField(selectStatement, index, schema, value, colDef);
  }

  private void close()
  throws SQLException {
    log.debug(
        "Closing JdbcRecordSelector with selectPreparedStatement: {}", selectStatement);
    if (selectStatement != null) {
      selectStatement.close();
      selectStatement = null;
    }
  }

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
                .map(name -> new ColumnId(tableId, name))
                .collect(Collectors.toList());
  }

}
