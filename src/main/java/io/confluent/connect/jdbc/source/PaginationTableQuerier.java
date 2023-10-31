package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class PaginationTableQuerier extends TableQuerier {

  private final int pageSize;
  private final List<ColumnId> keyColumns;
  private final PaginationCriteria criteria;

  private List<Object> start = new ArrayList<>();
  private List<Object> end = new ArrayList<>();
  private boolean isLastPage = false;

  public PaginationTableQuerier(DatabaseDialect dialect,
                                QueryMode mode,
                                String nameOrQuery,
                                String topicPrefix,
                                String suffix,
                                int pageSize,
                                List<String> keySet) {
    super(dialect, mode, nameOrQuery, topicPrefix, suffix);
    this.pageSize = pageSize;
    this.keyColumns = keySet.stream().map(key -> new ColumnId(this.tableId, key)).collect(Collectors.toList());
    this.criteria = new PaginationCriteria(this.keyColumns);
  }

  @Override
  protected void createPreparedStatement(Connection db)
  throws SQLException {
    this.queryNextPagination();
    log.info("GotThePage: [{}], [{}]", start, end);

    ExpressionBuilder builder = dialect.expressionBuilder();
    builder.append("SELECT * FROM ").append(tableId);
    criteria.whereClause(builder, start, PaginationCriteria.Comparator.LARGER, end, PaginationCriteria.Comparator.LESS_OR_EQUAL, suffix);
    criteria.orderClause(builder, PaginationCriteria.Order.ASC);

    String queryStr = builder.toString();

    recordQuery(query);
    log.info("{} prepared SQL query: {}", this, queryStr);
    stmt = dialect.createPreparedStatement(db, queryStr);
    criteria.setQueryParameters(stmt, start, end);
  }

  private void queryNextPagination()
  throws SQLException {
    ExpressionBuilder builder = dialect.expressionBuilder();
    builder.append("SELECT ").appendList().of(keyColumns).append(" FROM ").append(tableId);
    criteria.whereClause(builder, start, PaginationCriteria.Comparator.LARGER, null, null, null);
    criteria.orderClause(builder, PaginationCriteria.Order.ASC);
    criteria.limitClause(builder, pageSize - 1, 1);

    String queryStr = builder.toString();

    log.info("{} prepared SQL to query next page: {}", this, queryStr);
    try (PreparedStatement pstmt = dialect.createPreparedStatement(db, queryStr)) {
      criteria.setQueryParameters(pstmt, start);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          List<Object> end = new ArrayList<>();
          for (ColumnId keyColumn : keyColumns) {
            end.add(rs.getObject(keyColumn.name()));
          }
          if (!isLastPage) {

          }

        } else {

        }


//        if (rs.next()) {
//          List<Object> end = new ArrayList<>();
//          for (ColumnId keyColumn : keyColumns) {
//            end.add(rs.getObject(keyColumn.name()));
//          }
//          this.start = this.end;
//          this.end = end;
//        } else {
//          // Here comes to the last page,
//          if (!this.end.isEmpty()) {
//            this.start = this.end;
//            this.end = new ArrayList<>();
//          }
//        }
      }
    }
  }

  @Override
  protected ResultSet executeQuery()
  throws SQLException {
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord()
  throws SQLException {
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
    String name = tableId.tableName(); // backwards compatible
    String topic = topicPrefix + name;
    Map<String, String> partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, name);
    return new SourceRecord(partition, null, topic, record.schema(), record);
  }

}
