package io.confluent.connect.jdbc.segment.worker.consumer;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.segment.helper.SegmentCriteria;
import io.confluent.connect.jdbc.segment.model.Segment;
import io.confluent.connect.jdbc.segment.model.SegmentResult;
import io.confluent.connect.jdbc.segment.queue.SegmentQueue;
import io.confluent.connect.jdbc.segment.worker.SegmentConsumer;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ConnectionProvider;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class SegmentQuerier extends SegmentConsumer {

  private final BlockingQueue<SegmentResult<ResultSet>> results;

  private final SegmentCriteria.Order keyOrder = SegmentCriteria.Order.ASC;

  public SegmentQuerier(
    String name,
    TableId tableId,
    List<ColumnId> keyColumns,
    List<ColumnId> nonKeyColumns,
    Connection db,
    DatabaseDialect dialect,
    SegmentQueue queue,
    SegmentCriteria criteria,
    String filter,
    BlockingQueue<SegmentResult<ResultSet>> results
  ) {
    super(name,
          tableId,
          keyColumns,
          nonKeyColumns,
          db,
          dialect,
          queue,
          criteria,
          filter);
    this.results = results;
  }

  @Override
  public void onSegment(Segment segment)
  throws Exception {
    String queryExpression = buildSegmentSelectExpression(segment);
    try (PreparedStatement stmt = dialect.createPreparedStatement(db, queryExpression);) {
      criteria.setQueryParameters(stmt, segment.from, segment.to);
      log.debug("Selector on segment[{}]", segment);
      SegmentResult<ResultSet> result = new SegmentResult<>(segment, stmt.executeQuery());
      results.put(result);
      log.debug("Selector successfully retrieve resultSet");
    }
  }

  private String buildSegmentSelectExpression(Segment segment) {
    ExpressionBuilder builder = dialect.expressionBuilder();
    builder.append("SELECT ");
    builder.appendList()
           .of(keyColumns, nonKeyColumns);
    builder.append(" FROM ");
    builder.append(tableId);
    criteria.whereClause(builder, segment.from, SegmentCriteria.Comparator.LARGER, segment.to, SegmentCriteria.Comparator.LESS_OR_EQUAL, filter);
    criteria.orderClause(builder, keyOrder);
    return builder.toString();
  }


}
