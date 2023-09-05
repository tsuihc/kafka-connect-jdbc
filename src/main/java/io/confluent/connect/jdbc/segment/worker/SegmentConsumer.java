package io.confluent.connect.jdbc.segment.worker;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.segment.helper.SegmentCriteria;
import io.confluent.connect.jdbc.segment.model.Segment;
import io.confluent.connect.jdbc.segment.queue.SegmentQueue;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;

import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class SegmentConsumer extends SegmentWorker {

  protected final boolean orderSensitive;

  public SegmentConsumer(
    String name,
    TableId tableId,
    List<ColumnId> keyColumns,
    List<ColumnId> nonKeyColumns,
    ConnectionProvider dbProvider,
    DatabaseDialect dialect,
    SegmentQueue queue,
    SegmentCriteria criteria,
    String filter,
    boolean orderSensitive
  ) {
    super(name,
          tableId,
          keyColumns,
          nonKeyColumns,
          dbProvider,
          dialect,
          queue,
          criteria,
          filter
    );
    this.orderSensitive = orderSensitive;
  }

  @Override
  public Segment fetchSegment() throws Exception {
    Segment segment;
    while ((segment = queue.poll(100, TimeUnit.MILLISECONDS)) == null) {
      if (queue.isComplete()) {
        return null;
      }
    }
    return segment;
  }

}
