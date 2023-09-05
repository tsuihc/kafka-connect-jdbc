package io.confluent.connect.jdbc.segment.model;

import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class Segment {

  public final List<Object> from;
  public final List<Object> to;

  @Override
  public String toString() {
    return "Segment: { \n" +
      "  from: [" + from + "]\n" +
      "  to  : [" + to + "]\n" +
      "}";
  }
}
