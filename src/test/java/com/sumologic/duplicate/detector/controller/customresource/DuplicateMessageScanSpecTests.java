package com.sumologic.duplicate.detector.controller.customresource;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DuplicateMessageScanSpecTests {

  @Test
  void getSegments() {
    DuplicateMessageScanSpec spec = new DuplicateMessageScanSpec("2023-09-06T10:00:00-07:00",
      "2023-09-06T10:15:00-07:00", List.of("0000000000000005"));

    assertIterableEquals(
      List.of(Pair.of("2023-09-06T10:00:00-07:00", "2023-09-06T10:15:00-07:00")),
      spec.splitTimeRange());

    spec.timeRangeSegmentLength = "PT5m";
    assertIterableEquals(List.of(
      Pair.of("2023-09-06T17:00:00Z", "2023-09-06T17:05:00Z"),
      Pair.of("2023-09-06T17:05:00Z", "2023-09-06T17:10:00Z"),
      Pair.of("2023-09-06T17:10:00Z", "2023-09-06T17:15:00Z")
      ), spec.splitTimeRange());

    spec.endTime = "2023-09-06T10:18:00-07:00";
    assertIterableEquals(List.of(
      Pair.of("2023-09-06T17:00:00Z", "2023-09-06T17:05:00Z"),
      Pair.of("2023-09-06T17:05:00Z", "2023-09-06T17:10:00Z"),
      Pair.of("2023-09-06T17:10:00Z", "2023-09-06T17:15:00Z"),
      Pair.of("2023-09-06T17:15:00Z", "2023-09-06T17:18:00Z")
      ), spec.splitTimeRange());
  }

  @Test
  void zipCustomersAndSegments() {
    DuplicateMessageScanSpec spec = new DuplicateMessageScanSpec("2023-09-06T10:00:00-07:00",
      "2023-09-06T10:15:00-07:00", List.of("0000000000000005"));

    assertIterableEquals(
      List.of(
        new Segment("0", "0000000000000005", "2023-09-06T10:00:00-07:00", "2023-09-06T10:15:00-07:00")
      ), spec.getSegments());

    spec.customers = List.of("0000000000000005", "0000000000000006");
    assertIterableEquals(
      List.of(
        new Segment("0", "0000000000000005", "2023-09-06T10:00:00-07:00", "2023-09-06T10:15:00-07:00"),
        new Segment("1", "0000000000000006", "2023-09-06T10:00:00-07:00", "2023-09-06T10:15:00-07:00")
      ), spec.getSegments());

    spec.timeRangeSegmentLength = "PT5m";
    assertIterableEquals(
      List.of(
        new Segment("0", "0000000000000005", "2023-09-06T17:00:00Z", "2023-09-06T17:05:00Z"),
        new Segment("1", "0000000000000005", "2023-09-06T17:05:00Z", "2023-09-06T17:10:00Z"),
        new Segment("2", "0000000000000005", "2023-09-06T17:10:00Z", "2023-09-06T17:15:00Z"),
        new Segment("3", "0000000000000006", "2023-09-06T17:00:00Z", "2023-09-06T17:05:00Z"),
        new Segment("4", "0000000000000006", "2023-09-06T17:05:00Z", "2023-09-06T17:10:00Z"),
        new Segment("5", "0000000000000006", "2023-09-06T17:10:00Z", "2023-09-06T17:15:00Z")
      ), spec.getSegments());
  }
}
