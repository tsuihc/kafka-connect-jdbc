package io.confluent.connect.jdbc.segment.helper;

import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class SegmentCriteria {

  protected final Collection<ColumnId> keyColumns;

  @RequiredArgsConstructor
  public enum Comparator {
    LARGER(">"),
    LARGER_OR_EQUAL(">="),
    EQUAL("="),
    LESS("<"),
    LESS_OR_EQUAL("<=");

    private final String symbol;
  }

  @RequiredArgsConstructor
  public enum Order {
    ASC("asc"), DESC("desc");

    private final String symbol;
  }

  public void whereClause(
    ExpressionBuilder builder,
    List<Object> segmentStart,
    Comparator compareToStart,
    List<Object> segmentEnd,
    Comparator compareToEnd,
    String filter
  ) {
    boolean withSegmentStart = segmentStart != null && segmentStart.size() > 0;
    boolean withSegmentEnd = segmentEnd != null && segmentEnd.size() > 0;
    boolean withFilter = filter != null && !filter.isEmpty();
    if (!withSegmentStart && !withSegmentEnd && !withFilter) {
      return;
    }

    builder.append(" WHERE ");
    if (withSegmentStart) {
      builder.appendList()
             .bracketed()
             .of(keyColumns);
      builder.append(compareToStart.symbol);
      builder.appendList()
             .bracketed()
             .of(Collections.nCopies(keyColumns.size(), "?"));
      builder.append(" AND ");
    }
    if (withSegmentEnd) {
      builder.appendList()
             .bracketed()
             .of(keyColumns);
      builder.append(compareToEnd.symbol);
      builder.appendList()
             .bracketed()
             .of(Collections.nCopies(keyColumns.size(), "?"));
      builder.append(" AND ");
    }
    if (withFilter) {
      builder.append(filter);
      builder.append(" AND ");
    }
    builder.delete(builder.length() - 5, builder.length()); // delete the last dummy " AND "
  }

  public void orderClause(
    ExpressionBuilder builder,
    Order order
  ) {
    builder.append(" ORDER BY ")
           .appendList()
           .transformedBy(ExpressionBuilder.columnNamesWith(" " + order.symbol))
           .of(keyColumns);
  }

  public void limitClause(
    ExpressionBuilder builder,
    int offset,
    int limit
  ) {
    builder.append(" LIMIT ");
    if (offset > 0) {
      builder.append(offset).append(",");
    }
    builder.append(limit);
  }

  @SafeVarargs
  public final void setQueryParameters(
    PreparedStatement stmt,
    List<Object>... parameters
  )
  throws SQLException {
    List<Object> objects = Stream.of(parameters).flatMap(Collection::stream).collect(Collectors.toList());
    for (int i = 0; i < objects.size(); i++) {
      stmt.setObject(i + 1, objects.get(i));
    }
  }

}
