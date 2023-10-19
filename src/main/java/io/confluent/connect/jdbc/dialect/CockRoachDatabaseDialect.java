package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;

import java.util.Collection;

public class CockRoachDatabaseDialect extends PostgreSqlDatabaseDialect {

  public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
    public Provider() {
      super(CockRoachDatabaseDialect.class.getSimpleName(), "cockroach");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new CockRoachDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public CockRoachDatabaseDialect(AbstractConfig config) {
    super(config);
  }

  @Override
  public String buildUpsertQueryStatement(TableId table,
                                          Collection<ColumnId> keyColumns,
                                          Collection<ColumnId> nonKeyColumns,
                                          TableDefinition definition) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("UPSERT INTO ");
    builder.append(table);
    builder.append(" (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(this.columnValueVariables(definition))
           .of(keyColumns, nonKeyColumns);
    builder.append(")");
    return builder.toString();
  }

}
