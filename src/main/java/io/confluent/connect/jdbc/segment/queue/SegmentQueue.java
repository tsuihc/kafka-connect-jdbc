package io.confluent.connect.jdbc.segment.queue;

import io.confluent.connect.jdbc.segment.model.Segment;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.concurrent.BlockingQueue;

@RequiredArgsConstructor
public abstract class SegmentQueue implements BlockingQueue<Segment> {

  protected final int size;
  @Getter
  @Setter
  private boolean complete = false;

}
