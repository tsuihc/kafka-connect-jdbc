package io.confluent.connect.jdbc.segment.helper;

import io.confluent.connect.jdbc.segment.queue.SegmentQueue;
import io.confluent.connect.jdbc.segment.worker.SegmentWorker;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
public class SegmentWorkerScheduler {

  private final List<SegmentWorker> producers;
  private final SegmentQueue segmentQueue;
  private final List<SegmentWorker> consumers;

  private ExecutorService executorService;
  @Getter
  private Exception exception;

  private volatile boolean running = false;

  public void start() {
    running = true;
    executorService = new ThreadPoolExecutor(producers.size() + consumers.size(),
                                             producers.size() + consumers.size(),
                                             1000L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
    for (SegmentWorker producer : producers) {
      executorService.submit(producer);
    }
    for (SegmentWorker consumer : consumers) {
      executorService.submit(consumer);
    }

    Thread monitorThread = new Thread(() -> {
      while (running) {
        boolean producerComplete = true;
        for (SegmentWorker producer : producers) {
          monitSegmentWorker(producer);
          producerComplete &= producer.isComplete();
        }
        segmentQueue.setComplete(producerComplete);
        for (SegmentWorker consumer : consumers) {
          monitSegmentWorker(consumer);
        }
        if (exception != null) {
          this.stop();
          return;
        }
        try {
          //noinspection BusyWait
          Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }
      }
    });
    monitorThread.start();
  }

  public void monitSegmentWorker(SegmentWorker worker) {
    if (worker.isExceptionThrown()) {
      if (this.exception == null) {
        this.exception = new RuntimeException(worker.getCause());
      } else {
        this.exception.addSuppressed(worker.getCause());
      }
    }
  }

  public void stop() {
    running = false;
    producers.forEach(SegmentWorker::shutdown);
    consumers.forEach(SegmentWorker::shutdown);
    executorService.shutdown();
    segmentQueue.clear();
  }

  public boolean isComplete() {
    return segmentQueue.isComplete();
  }
}
