package com.sumologic.duplicate.detector.controller.customresource;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DuplicateMessageScanStatusTests {

  @Test
  void testFailed() {
    List<Segment> segments = createSegments();
    segments.get(0).processing();
    DuplicateMessageScanStatus status = new DuplicateMessageScanStatus(segments);
    status.failed("error");
    assertEquals("error", status.error);
    assertFalse(status.successful);
    assertTrue(status.failed);
    assertAll("All segments marked as failed",
      () -> assertEquals(Segment.SegmentStatus.FAILED, status.segments.get(0).status),
      () -> assertEquals(Segment.SegmentStatus.FAILED, status.segments.get(1).status));
    assertEquals(0, status.pendingSegmentCount);
    assertEquals(0, status.processingSegmentCount);
    assertEquals(0, status.completedSegmentCount);
    assertEquals(2, status.failedSegmentCount);

    reset(status);
    segments.get(0).completed();
    status.failed("error");
    assertAll("Completed segments left untouched",
      () -> assertEquals(Segment.SegmentStatus.COMPLETED, status.segments.get(0).status),
      () -> assertEquals(Segment.SegmentStatus.FAILED, status.segments.get(1).status));
    assertEquals(0, status.pendingSegmentCount);
    assertEquals(0, status.processingSegmentCount);
    assertEquals(1, status.completedSegmentCount);
    assertEquals(1, status.failedSegmentCount);
  }

  @Test
  void testCompleted() {
    List<Segment> segments = createSegments();
    segments.get(0).processing();
    DuplicateMessageScanStatus status = new DuplicateMessageScanStatus(segments);
    status.completed();
    assertNull(status.error);
    assertTrue(status.successful);
    assertFalse(status.failed);
    assertAll("All segments marked as completed",
      () -> assertEquals(Segment.SegmentStatus.COMPLETED, status.segments.get(0).status),
      () -> assertEquals(Segment.SegmentStatus.COMPLETED, status.segments.get(1).status));
    assertEquals(0, status.pendingSegmentCount);
    assertEquals(0, status.processingSegmentCount);
    assertEquals(2, status.completedSegmentCount);
    assertEquals(0, status.failedSegmentCount);

    reset(status);
    segments.get(0).failed();
    status.completed();
    assertAll("Failed segments left untouched",
      () -> assertEquals(Segment.SegmentStatus.FAILED, status.segments.get(0).status),
      () -> assertEquals(Segment.SegmentStatus.COMPLETED, status.segments.get(1).status));
    assertEquals(0, status.pendingSegmentCount);
    assertEquals(0, status.processingSegmentCount);
    assertEquals(1, status.completedSegmentCount);
    assertEquals(1, status.failedSegmentCount);
  }

  @Test
  void testUpdateSegmentsCounts() {
    List<Segment> segments = createSegments();
    DuplicateMessageScanStatus status = new DuplicateMessageScanStatus(segments);
    assertEquals(2, status.segmentCount);
    assertEquals(2, status.pendingSegmentCount);
    assertEquals(0, status.processingSegmentCount);
    assertEquals(0, status.completedSegmentCount);
    assertEquals(0, status.failedSegmentCount);

    segments.get(0).processing();
    status.updateSegmentsCounts();
    assertEquals(2, status.segmentCount);
    assertEquals(1, status.pendingSegmentCount);
    assertEquals(1, status.processingSegmentCount);
    assertEquals(0, status.completedSegmentCount);
    assertEquals(0, status.failedSegmentCount);

    segments.get(0).completed();
    status.updateSegmentsCounts();
    assertEquals(2, status.segmentCount);
    assertEquals(1, status.pendingSegmentCount);
    assertEquals(0, status.processingSegmentCount);
    assertEquals(1, status.completedSegmentCount);
    assertEquals(0, status.failedSegmentCount);

    segments.get(1).failed();
    status.updateSegmentsCounts();
    assertEquals(2, status.segmentCount);
    assertEquals(0, status.pendingSegmentCount);
    assertEquals(0, status.processingSegmentCount);
    assertEquals(1, status.completedSegmentCount);
    assertEquals(1, status.failedSegmentCount);
  }

  List<Segment> createSegments() {
    return List.of(new Segment("0000000000000005", "2023-09-06T10:00:00-07:00", "2023-09-06T10:15:00-07:00"),
      new Segment("0000000000000006", "2023-09-06T10:00:00-07:00", "2023-09-06T10:15:00-07:00"));
  }

  void reset(DuplicateMessageScanStatus status) {
    status.failed = false;
    status.successful = false;
    status.error = null;
    status.segments.forEach(s -> s.status = Segment.SegmentStatus.PENDING);
  }
}
