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

  private static final String DUMMY_QUERY =  "SELECT 1 WHERE 1 != 1";

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
    if (!this.queryNextPagination()) {
      stmt = dialect.createPreparedStatement(db, DUMMY_QUERY);
      return;
    }

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

  private boolean queryNextPagination()
  throws SQLException {
    ExpressionBuilder builder = dialect.expressionBuilder();
    builder.append("(SELECT ").append(" 'page' as datum, ");
    builder.appendList().of(keyColumns);
    builder.append(" FROM ").append(tableId);
    criteria.whereClause(builder, end, PaginationCriteria.Comparator.LARGER, null, null, null);
    criteria.orderClause(builder, PaginationCriteria.Order.ASC);
    criteria.limitClause(builder, pageSize - 1, 1);
    builder.append(")");
    builder.append(" UNION ");
    builder.append("(SELECT").append(" 'end' as datum, ");
    builder.appendList().of(keyColumns);
    builder.append(" FROM ").append(tableId);
    criteria.orderClause(builder, PaginationCriteria.Order.DESC);
    criteria.limitClause(builder, 1);
    builder.append(")");

    String queryStr = builder.toString();

    log.info("{} prepared SQL to query next page: {}", this, queryStr);
    try (PreparedStatement pstmt = dialect.createPreparedStatement(db, queryStr)) {
      criteria.setQueryParameters(pstmt, end);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (!rs.next()) {
          // the result set is empty, which means the table is empty.
          log.info("{} got an empty table {}", this, tableId);
          return false;
        }

        List<Object> datum = new ArrayList<>();
        for (ColumnId keyColumn : keyColumns) {
          datum.add(rs.getObject(keyColumn.name()));
        }
        if (rs.getString("datum").equals("page")) {
          // there is a pagination
          this.start = this.end;
          this.end = datum;
        } else if (rs.getString("datum").equals("end")) {
          // has seeked to the end of the table
          // it is determined whether the last page has been fetched yet.
          boolean fetched = this.end.equals(datum);
          this.start = this.end;
          if (!fetched) {
            this.end = datum;
          }
        } else {
          throw new RuntimeException("Unknown datum mark: " + rs.getString("datum") + ", which is a program error!");
        }
        return true;
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
