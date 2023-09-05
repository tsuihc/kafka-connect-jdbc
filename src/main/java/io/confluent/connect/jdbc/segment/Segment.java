package io.confluent.connect.jdbc.segment;

import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class Segment {

  final List<Object> from;
  final List<Object> to;

  @Override
  public String toString() {
    return "Segment: { \n" +
      "  from: [" + from + "]\n" +
      "  to  : [" + to + "]\n" +
      "}";
  }
}
