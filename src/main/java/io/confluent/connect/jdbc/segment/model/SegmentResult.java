package io.confluent.connect.jdbc.segment.model;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SegmentResult<T> {

  public final Segment segment;
  public final T value;

  public T value() {
    return value;
  }

}