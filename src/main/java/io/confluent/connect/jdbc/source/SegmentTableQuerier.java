package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.segment.helper.SegmentCriteria;
import io.confluent.connect.jdbc.segment.helper.SegmentWorkerScheduler;
import io.confluent.connect.jdbc.segment.model.SegmentResult;
import io.confluent.connect.jdbc.segment.queue.MemorySegmentQueue;
import io.confluent.connect.jdbc.segment.queue.SegmentQueue;
import io.confluent.connect.jdbc.segment.worker.SegmentProducer;
import io.confluent.connect.jdbc.segment.worker.SegmentWorker;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Query JDBC table in segment
 *
 * @author hcxu0814@gmail.com
 */
@Slf4j
public class SegmentTableQuerier extends TableQuerier {

  private final int segmentSize;
  private final int concurrency;
  private final BlockingQueue<SegmentResult<ResultSet>> resultSets;
  private final String filter;
  private final String accessMode;
  private final boolean orderSensitive;

  private ResultSet currentResultSet;
  private SegmentWorkerScheduler scheduler;

  private volatile boolean running = false;
  private SchemaMapping schemaMapping;

  public SegmentTableQuerier(
    DatabaseDialect dialect,
    String name,
    int segmentSize,
    int concurrency,
    int maxWaitedResultSetsNumber,
    String filter,
    String accessMode,
    boolean orderSensitive
  ) {
    super(dialect, name);
    this.segmentSize = segmentSize;
    this.concurrency = concurrency;
    this.resultSets = new ArrayBlockingQueue<>(maxWaitedResultSetsNumber);
    this.filter = filter;
    this.accessMode = accessMode;
    this.orderSensitive = orderSensitive;
  }

  @Override
  public boolean querying() {
    return running;
  }

  @Override
  public synchronized void maybeStartQuery(Connection db) throws SQLException {
    if (running) {
      return;
    }
    // mark running as TRUE
    running = true;
    this.db = db;
    List<ColumnId> keyColumns = new ArrayList<>();
    List<ColumnId> nonKeyColumns = new ArrayList<>();
    Map<ColumnId, ColumnDefinition> columnDefns = new LinkedHashMap<>();
    dialect.describeColumns(db,
                            tableId.catalogName(),
                            tableId.schemaName(),
                            tableId.tableName(),
                            null
    ).forEach(((columnId, columnDefn) -> {
      if (columnDefn.isPrimaryKey()) {
        keyColumns.add(columnId);
      } else {
        nonKeyColumns.add(columnId);
      }
      columnDefns.put(columnId, columnDefn);
    }));
    schemaMapping = SchemaMapping.create(tableId.tableName(), columnDefns, dialect);
    SegmentCriteria criteria = dialect.criteriaFor(keyColumns);
    SegmentQueue queue = new MemorySegmentQueue(100);
    SegmentWorker slicer = new SegmentProducer(
      tableId + "-slicer-0",
      tableId,
      keyColumns,
      nonKeyColumns,
      null,
      dialect,
      queue,
      criteria,
      filter,
      segmentSize,
      Collections.emptyList()
    );
    List<SegmentWorker> consumers = new ArrayList<>();
    for (int i = 0; i < concurrency; i++) {
      SegmentWorker consumer;
      if (accessMode.equalsIgnoreCase("query")) {
        consumer = new SegmentSelector(
          tableId.toString() + "-selector-" + i,
          tableId,
          keyColumns,
          nonKeyColumns,
          dbProvider,
          dialect,
          queue,
          criteria,
          filter,
          resultSets,
          orderSensitive
        );
      } else if (accessMode.equalsIgnoreCase("delete")) {
        consumer = new SegmentDeleter(
          tableId + "-deleter-" + i,
          tableId,
          keyColumns,
          nonKeyColumns,
          dbProvider,
          dialect,
          queue,
          criteria,
          filter
        );
      } else {
        throw new IllegalStateException("Unsupported access mode: " + accessMode);
      }
      consumers.add(consumer);
    }
    scheduler = new SegmentWorkerScheduler(
      Collections.singletonList(slicer),
      queue,
      consumers
    );
    scheduler.start();
  }

  @Override
  public boolean next() throws SQLException {
    if (currentResultSet != null && !currentResultSet.isClosed() && currentResultSet.next()) {
      return true;
    }
    if (nextNotEmptyResultSet()) {
      return this.next();
    }
    return false;
  }

  public boolean nextNotEmptyResultSet() throws SQLException {
    if (currentResultSet != null) {
      closeResultSet(currentResultSet);
    }
    while (running) {
      if (scheduler.getException() != null) {
        throw new SQLException(scheduler.getException());
      }
      try {
        SegmentResult<ResultSet> result = resultSets.poll(2, TimeUnit.SECONDS);
        if (result != null) {
          currentResultSet = result.value();
          if (!currentResultSet.isBeforeFirst()) {
            closeResultSet(currentResultSet);
            currentResultSet = null;
            continue;
          }
          return true;
        }
      } catch (InterruptedException ignored) {
      }
      if (scheduler.isComplete()) {
        return false;
      }
    }
    return false;
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    Struct struct = JdbcSourceHelper.extractStructFromResultSet(schemaMapping, currentResultSet);
    String name = tableId.tableName();
    return new SourceRecord(null,
                            null,
                            name,
                            null,
                            null,
                            struct.schema(),
                            struct);
  }

  @Override
  public void reset(
    long now,
    boolean resetOffset
  ) {
    log.debug("Resetting...");
    this.running = false;
    this.scheduler.stop();
    this.closeResultSetQuietly(currentResultSet);
    Iterator<SegmentResult<ResultSet>> it = this.resultSets.iterator();
    while (it.hasNext()) {
      closeResultSetQuietly(it.next().value());
      it.remove();
    }
  }

  private void closeResultSet(ResultSet resultSet) throws SQLException {
    try {
      if (resultSet != null) {
        if (resultSet.getStatement() != null) {
          resultSet.getStatement().close();
        }
        resultSet.close();
      }
    } catch (SQLException e) {
      if (!resultSet.isClosed()) {
        throw e;
      }
    }
  }

  private void closeResultSetQuietly(ResultSet resultSet) {
    try {
      closeResultSet(resultSet);
    } catch (SQLException ignored) {
    }
  }

  private boolean isRunning() {
    return running;
  }

  @Override
  protected boolean endlessQuerying() {
    return false;
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    throw new UnsupportedOperationException();
  }

}
