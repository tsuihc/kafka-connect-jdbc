package io.confluent.connect.jdbc.segment.worker;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.segment.model.Segment;
import io.confluent.connect.jdbc.segment.helper.SegmentCriteria;
import io.confluent.connect.jdbc.segment.queue.SegmentQueue;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public abstract class SegmentWorker implements Runnable {
  protected final String name;
  protected final TableId tableId;
  protected final List<ColumnId> keyColumns;
  protected final List<ColumnId> nonKeyColumns;
  protected final Connection db;
  protected final DatabaseDialect dialect;
  protected final SegmentQueue queue;
  protected final SegmentCriteria criteria;
  protected final String filter;

  protected volatile boolean initialized;
  protected volatile boolean running;
  protected volatile boolean complete;
  protected volatile Throwable throwable;

  @Override
  public void run() {
    Thread.currentThread().setName(name);

    initialized = false;
    running = true;
    complete = false;
    throwable = null;

    while (running) {
      if (this.throwable != null) {
        continue;
      }
      try {
        if (!initialized) {
          init();
          initialized = true;
        }
        Segment segment = fetchSegment();
        log.info("Successfully fetch segment[{}]", segment);
        if (segment == null) {
          complete = true;
          running = false;
          return;
        }
        beforeSegment(segment);
        onSegment(segment);
        afterSegment(segment);
      } catch (Throwable throwable) {
        this.throwable = throwable;
        onThrowable(throwable);
      }
    }
  }

  public void init()
  throws Exception {
    db.setAutoCommit(true);
  }

  public abstract Segment fetchSegment()
  throws Exception;

  public void beforeSegment(Segment segment)
  throws Exception {
  }

  public void onSegment(Segment segment)
  throws Exception {
  }

  public void afterSegment(Segment segment)
  throws Exception {
  }

  public void onThrowable(Throwable throwable) {
    log.error("SegmentWorker-[{}]-[{}] on throwable",
              this.getClass().getName(),
              this.name,
              throwable);
  }

  public boolean isExceptionThrown() {
    return this.throwable != null;
  }

  public Throwable getCause() {
    return this.throwable;
  }

  public void recoveryFromException() {
    this.throwable = null;
  }

  public boolean isComplete() {
    return this.complete;
  }

  public void shutdown() {
    log.debug("[{}] Shutting Down...", name);
    this.running = false;
    if (this.db != null) {
      try {
        this.db.close();
      } catch (Exception e) {
        log.error("Error when close Database connection", e);
      }
    }
  }

}
