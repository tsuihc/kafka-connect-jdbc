package io.confluent.connect.jdbc.select;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.TableAlterOrCreateException;
import io.confluent.connect.jdbc.source.SchemaMapping;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.ValueToKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class JdbcRecordSelector {
  private static final Logger log = LoggerFactory.getLogger(JdbcRecordSelector.class);

  private final TableId tableId;
  private final List<ColumnId> fields;
  private final DatabaseDialect dialect;
  private final Connection connection;

  private Schema keySchema;
  private PreparedStatement selectStatement;
  private TableDefinition tableDef;
  private SchemaMapping schemaMapping;
  private Transformation<SourceRecord> valueToKeyTransform;

  public JdbcRecordSelector(
    TableId tableId,
    List<String> fields,
    DatabaseDialect dialect,
    Connection connection
  ) {
    this.tableId = tableId;
    this.fields = fields.stream().map(field -> new ColumnId(tableId, field)).collect(Collectors.toList());
    this.dialect = dialect;
    this.connection = connection;
  }

  public SourceRecord select(SchemaAndValue keys)
  throws SQLException, TableAlterOrCreateException {
    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, keys.schema())) {
      keySchema = keys.schema();
      schemaChanged = true;
    }
    if (schemaChanged || selectStatement == null) {
      // re-initialize everything
      List<ColumnId> keyColumns = getKeyColumns(keySchema);
      if (valueToKeyTransform == null) {
        valueToKeyTransform = new ValueToKey<>();
      }
      Map<String, Object> transformationConfig = new HashMap<>();
      transformationConfig.put(ValueToKey.FIELDS_CONFIG, getKeyNames(keySchema));
      valueToKeyTransform.configure(transformationConfig);

      String sql = dialect.buildSelectStatement(tableId, fields, keyColumns);

      close();
      selectStatement = dialect.createPreparedStatement(connection, sql);
      tableDef = dialect.describeTable(connection, tableId);
    }
    bindKeyFields(keys);
    SourceRecord record = executeSelect();
    // if the record is not null, extract value to key by transform
    return record == null ? null : valueToKeyTransform.apply(record);
  }

  private SourceRecord executeSelect()
  throws SQLException {
    ResultSet resultSet = selectStatement.executeQuery();
    if (!resultSet.next()) {
      return null;
    }
    if (schemaMapping == null) {
      String schemaName = tableId != null ? tableId.tableName() : null; // backwards compatible
      schemaMapping = SchemaMapping.create(schemaName, resultSet.getMetaData(), dialect);
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

  protected void bindKeyFields(SchemaAndValue keys)
  throws SQLException {
    int index = 1;
    if (keys.schema().type().isPrimitive()) {
      bindField(index, keys.schema(), keys.value(), keys.schema().name());
    } else {
      for (Field field : keys.schema().fields()) {
        bindField(index++, field.schema(), ((Struct) keys.value()).get(field), field.name());
      }
    }
  }

  protected void bindField(int index,
                           Schema schema,
                           Object value,
                           String fieldName)
  throws SQLException {
    ColumnDefinition columnDef = tableDef.definitionForColumn(fieldName);
    dialect.bindField(selectStatement, index, schema, value, columnDef);
  }

  private void close()
  throws SQLException {
    log.debug("Closing JdbcRecordSelector with selectPreparedStatement: {}", selectStatement);
    if (selectStatement != null) {
      selectStatement.close();
      selectStatement = null;
    }
  }

  private List<ColumnId> getKeyColumns(Schema keySchema) {
    return getKeyNames(keySchema).stream()
                                 .map(keyColumn -> new ColumnId(tableId, keyColumn))
                                 .collect(Collectors.toList());
  }

  private List<String> getKeyNames(Schema keySchema) {
    List<String> keyNames = new ArrayList<>();
    if (keySchema.type().isPrimitive()) {
      keyNames.add(keySchema.schema().name());
    } else {
      for (Field field : keySchema.schema().fields()) {
        keyNames.add(field.name());
      }
    }
    return keyNames;
  }

}
