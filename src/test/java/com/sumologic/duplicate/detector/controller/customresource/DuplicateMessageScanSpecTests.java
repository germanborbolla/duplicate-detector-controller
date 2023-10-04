package com.sumologic.duplicate.detector.controller.customresource;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
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
      spec.getScanningSegments());

    spec.setTimeRangeSegmentLength("PT5m");
    assertIterableEquals(List.of(
      Pair.of("2023-09-06T17:00:00Z", "2023-09-06T17:05:00Z"),
      Pair.of("2023-09-06T17:05:00Z", "2023-09-06T17:10:00Z"),
      Pair.of("2023-09-06T17:10:00Z", "2023-09-06T17:15:00Z")
      ), spec.getScanningSegments());

    spec.setEndTime("2023-09-06T10:18:00-07:00");
    assertIterableEquals(List.of(
      Pair.of("2023-09-06T17:00:00Z", "2023-09-06T17:05:00Z"),
      Pair.of("2023-09-06T17:05:00Z", "2023-09-06T17:10:00Z"),
      Pair.of("2023-09-06T17:10:00Z", "2023-09-06T17:15:00Z"),
      Pair.of("2023-09-06T17:15:00Z", "2023-09-06T17:18:00Z")
      ), spec.getScanningSegments());
  }

  @Test
  void zipCustomersAndSegments() {
    DuplicateMessageScanSpec spec = new DuplicateMessageScanSpec("2023-09-06T10:00:00-07:00",
      "2023-09-06T10:15:00-07:00", List.of("0000000000000005"));

    assertIterableEquals(
      List.of(
        Triple.of("0000000000000005", "2023-09-06T10:00:00-07:00", "2023-09-06T10:15:00-07:00")
      ), spec.zipCustomerAndSegments());

    spec.setCustomers(List.of("0000000000000005", "0000000000000006"));
    assertIterableEquals(
      List.of(
        Triple.of("0000000000000005", "2023-09-06T10:00:00-07:00", "2023-09-06T10:15:00-07:00"),
        Triple.of("0000000000000006", "2023-09-06T10:00:00-07:00", "2023-09-06T10:15:00-07:00")
      ), spec.zipCustomerAndSegments());

    spec.setTimeRangeSegmentLength("PT5m");
    assertIterableEquals(
      List.of(
        Triple.of("0000000000000005", "2023-09-06T17:00:00Z", "2023-09-06T17:05:00Z"),
        Triple.of("0000000000000005", "2023-09-06T17:05:00Z", "2023-09-06T17:10:00Z"),
        Triple.of("0000000000000005", "2023-09-06T17:10:00Z", "2023-09-06T17:15:00Z"),
        Triple.of("0000000000000006", "2023-09-06T17:00:00Z", "2023-09-06T17:05:00Z"),
        Triple.of("0000000000000006", "2023-09-06T17:05:00Z", "2023-09-06T17:10:00Z"),
        Triple.of("0000000000000006", "2023-09-06T17:10:00Z", "2023-09-06T17:15:00Z")
      ), spec.zipCustomerAndSegments());
  }

  @Test
  void buildInputs() {
    DuplicateMessageScanSpec spec = new DuplicateMessageScanSpec("2023-09-06T10:00:00-07:00",
      "2023-09-06T10:15:00-07:00", List.of("0000000000000005"));

    assertIterableEquals(
      Map.of("duplicate_detector.properties", Map.of(
        DuplicateMessageScanSpec.CUSTOMERS_KEY, "0000000000000005",
        DuplicateMessageScanSpec.START_TIME_KEY, "2023-09-06T10:00:00-07:00",
        DuplicateMessageScanSpec.END_TIME_KEY, "2023-09-06T10:15:00-07:00",
        DuplicateMessageScanSpec.TARGET_OBJECT_KEY, "indices",
        DuplicateMessageScanSpec.WORKING_DIR_KEY, "/usr/sumo/system-tools/duplicate-detector-state",
        "duplicate_detector.onExitInvoke", "pkill fluent-bit")
      ).entrySet(), spec.buildInputs(true).entrySet());
    assertIterableEquals(
      Map.of("duplicate_detector.properties", Map.of(
        DuplicateMessageScanSpec.CUSTOMERS_KEY, "0000000000000005",
        DuplicateMessageScanSpec.START_TIME_KEY, "2023-09-06T10:00:00-07:00",
        DuplicateMessageScanSpec.END_TIME_KEY, "2023-09-06T10:15:00-07:00",
        DuplicateMessageScanSpec.TARGET_OBJECT_KEY, "indices",
        DuplicateMessageScanSpec.WORKING_DIR_KEY, "/usr/sumo/system-tools/duplicate-detector-state")
      ).entrySet(), spec.buildInputs(false).entrySet());

    spec.setTargetObject("blocks");
    assertIterableEquals(
      Map.of("duplicate_detector.properties", Map.of(
        DuplicateMessageScanSpec.CUSTOMERS_KEY, "0000000000000005",
        DuplicateMessageScanSpec.START_TIME_KEY, "2023-09-06T10:00:00-07:00",
        DuplicateMessageScanSpec.END_TIME_KEY, "2023-09-06T10:15:00-07:00",
        DuplicateMessageScanSpec.TARGET_OBJECT_KEY, "blocks",
        DuplicateMessageScanSpec.WORKING_DIR_KEY, "/usr/sumo/system-tools/duplicate-detector-state",
        "duplicate_detector.onExitInvoke", "pkill fluent-bit")
      ).entrySet(), spec.buildInputs(true).entrySet());

    spec.setCustomers(List.of("0000000000000005", "0000000000000006"));
    spec.setTargetObject(null);
    Map<String, Map<String, String>> inputs = spec.buildInputs(true);
    assertAll("first input", () -> assertTrue(inputs.containsKey("duplicate_detector-0.properties")), () -> {
      Map<String, String> input = inputs.get("duplicate_detector-0.properties");
      assertAll(() -> assertEquals("0000000000000005", input.get(DuplicateMessageScanSpec.CUSTOMERS_KEY)),
        () -> assertEquals("2023-09-06T10:00:00-07:00", input.get(DuplicateMessageScanSpec.START_TIME_KEY)),
        () -> assertEquals("2023-09-06T10:15:00-07:00", input.get(DuplicateMessageScanSpec.END_TIME_KEY)),
        () -> assertEquals("/usr/sumo/system-tools/duplicate-detector-state-0",
          input.get(DuplicateMessageScanSpec.WORKING_DIR_KEY)));
    });
    assertAll("second input", () -> assertTrue(inputs.containsKey("duplicate_detector-1.properties")), () -> {
      Map<String, String> input = inputs.get("duplicate_detector-1.properties");
      assertAll(() -> assertEquals("0000000000000006", input.get(DuplicateMessageScanSpec.CUSTOMERS_KEY)),
        () -> assertEquals("2023-09-06T10:00:00-07:00", input.get(DuplicateMessageScanSpec.START_TIME_KEY)),
        () -> assertEquals("2023-09-06T10:15:00-07:00", input.get(DuplicateMessageScanSpec.END_TIME_KEY)),
        () -> assertEquals("/usr/sumo/system-tools/duplicate-detector-state-1",
          input.get(DuplicateMessageScanSpec.WORKING_DIR_KEY)));
    });

  }
}
