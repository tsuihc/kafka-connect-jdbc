package io.confluent.connect.jdbc.segment.worker;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.segment.model.Segment;
import io.confluent.connect.jdbc.segment.helper.SegmentCriteria;
import io.confluent.connect.jdbc.segment.queue.SegmentQueue;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ConnectionProvider;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SegmentProducer extends SegmentWorker {

  private final int segmentSize;

  private List<Object> datum;
  private SegmentCriteria.Order order;
  private int offset;

  private static final List<Object> emptyDatum = Collections.emptyList();
  private static final int limit = 1;
  private static final String nonFilter = "";

  public SegmentProducer(
    String name,
    TableId tableId,
    List<ColumnId> keyColumns,
    List<ColumnId> nonKeyColumns,
    ConnectionProvider dbProvider,
    DatabaseDialect dialect,
    SegmentQueue queue,
    SegmentCriteria criteria,
    String filter,
    int segmentSize,
    List<Object> firstDatum
  ) {
    super(name,
          tableId,
          keyColumns,
          nonKeyColumns,
          dbProvider,
          dialect,
          queue,
          criteria,
          filter);
    this.segmentSize = segmentSize;
    this.datum = firstDatum;
    this.order = SegmentCriteria.Order.ASC;
    this.offset = 0;
  }

  @Override
  public Segment fetchSegment()
  throws Exception {
    List<Object> queriedSegmentEnd = querySegmentEnd();
    if (queriedSegmentEnd.isEmpty()) {
      order = SegmentCriteria.Order.DESC;
      offset = 0;
      queriedSegmentEnd = querySegmentEnd();
      if (queriedSegmentEnd.isEmpty()) {
        return null;
      }
    }
    return new Segment(datum, queriedSegmentEnd);
  }

  @Override
  public void onSegment(Segment segment)
  throws Exception {
    queue.put(segment);
  }

  @Override
  public void afterSegment(Segment segment)
  throws Exception {
    super.afterSegment(segment);
    datum = segment.to;
    order = SegmentCriteria.Order.ASC;
    offset = segmentSize;
  }

  private List<Object> querySegmentEnd()
  throws SQLException {
    String queryExpression = buildSegmentEndQueryExpression();
    try (PreparedStatement stmt = dialect.createPreparedStatement(db, queryExpression)) {
      criteria.setQueryParameters(stmt, datum);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          List<Object> queriedEndDatum = new ArrayList<>();
          for (int i = 0; i < keyColumns.size(); i++) {
            queriedEndDatum.add(rs.getObject(i + 1));
          }
          return queriedEndDatum;
        }
        return emptyDatum;
      }
    }
  }

  private String buildSegmentEndQueryExpression() {
    ExpressionBuilder builder = dialect.expressionBuilder();
    builder.append("SELECT ");
    builder.appendList().of(keyColumns);
    builder.append(" FROM ");
    builder.append(tableId);
    criteria.whereClause(builder, datum, SegmentCriteria.Comparator.LARGER, emptyDatum, SegmentCriteria.Comparator.LESS, nonFilter);
    criteria.orderClause(builder, order);
    criteria.limitClause(builder, offset, limit);
    return builder.toString();
  }

}
