package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanSpec;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanStatus;
import com.sumologic.duplicate.detector.controller.customresource.Segment;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DuplicateMessageScanReconcilerTests {

  @Mock
  private KubernetesClient client;
  @Mock
  private Context<DuplicateMessageScan> context;
  protected ReconcilerConfiguration configuration = new ReconcilerConfiguration(
    new ReconcilerConfiguration.JobConfiguration("image:123", true,
      5, true),
    new ReconcilerConfiguration.PersistentVolumeConfiguration("gp2", "100Mi"));
  private Instant clockInstant;
  private Clock clock = new MutableClock(() -> clockInstant, ZoneOffset.UTC.normalized());
  protected DuplicateMessageScanReconciler reconciler;

  private DuplicateMessageScanSpec spec = new DuplicateMessageScanSpec("2023-09-06T10:00:00-07:00",
    "2023-09-06T10:15:00-07:00", List.of("0000000000000005","0000000000000006"));
  private DuplicateMessageScan scan = new DuplicateMessageScan(spec);
  private ObjectMetaBuilder objectMetaBuilder = new ObjectMetaBuilder().withName("scan-3421-0");

  @Test
  void testNoJobs() {
    initJobs(Set.of());

    testUpdateStatus(spec.getSegments(), false, false, null,
      getClockDate(), getClockDate(), null);
  }

  @Test
  void testJobWithoutLabels() {
    initJobs(Set.of(jobWithMetadata(objectMetaBuilder.build())));

    testUpdateStatus(spec.getSegments(), false, false, null,
      getClockDate(), getClockDate(), null);
  }

  @Test
  void testJobForSegmentExistsNoStatus() {
    initJobs(Set.of(jobWithMetadata(metadataForSegment(0))));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.get(0).processing();
    testUpdateStatus(segments, false, false, null,
      getClockDate(), getClockDate(), null);
  }

  @Test
  void testJobForSegmentEmptyStatus() {
    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(0), new JobStatus())));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.get(0).processing();
    testUpdateStatus(segments, false, false, null,
      getClockDate(), getClockDate(), null);
  }

  @Test
  void testPendingJobForSegment() {
    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(0), jobStatus(0, 0))));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.get(0).processing();
    testUpdateStatus(segments, false, false, null,
      getClockDate(), getClockDate(), null);
  }

  @Test
  void testSucceededJobForSegment() {
    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(0), jobStatus(1, 0))));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.get(0).completed();
    testUpdateStatus(segments, false, false, null,
      getClockDate(), getClockDate(), null);
  }

  @Test
  void testFailedJobForSegment() {
    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(0), jobStatus(0, 1))));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.get(0).failed();
    testUpdateStatus(segments, false, false, null,
      getClockDate(), getClockDate(), null);
  }

  @Test
  void allSegmentsSucceeded() {
    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(0), jobStatus(1, 0))));
    reconciler.calculateStatus(scan, context);

    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(1), jobStatus(1, 0))));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.forEach(Segment::completed);
    testUpdateStatus(segments, true, false, null,
      getClockDate(), getClockDate(), getClockDate());
  }

  @Test
  void allSegmentsFinishedWithSomeFailures() {
    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(0), jobStatus(0, 1))));
    reconciler.calculateStatus(scan, context);

    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(1), jobStatus(1, 0))));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.get(0).failed();
    segments.get(1).completed();
    testUpdateStatus(segments, false, true, "One or more jobs failed",
      getClockDate(), getClockDate(), getClockDate());
  }

  @BeforeEach
  private void beforeEach() {
    clockInstant = Instant.now();
    reconciler = new DuplicateMessageScanReconciler(client, configuration, clock);
    scan = new DuplicateMessageScan(spec);
  }

  private void testUpdateStatus(List<Segment> expectedSegments,
                                boolean expectedSuccessful, boolean expectedFailed, String expectedError,
                                Date expectedScanStartedTime, Date expectedLastUpdateTime, Date expectedCompletionTime) {
    verifyResult(reconciler.calculateStatus(scan, context),
      expectedSegments, expectedSuccessful, expectedFailed, expectedError,
      expectedScanStartedTime, expectedLastUpdateTime, expectedCompletionTime);
  }

  private Job jobWithMetadata(ObjectMeta objectMeta) {
    return new JobBuilder().withMetadata(objectMeta).build();
  }

  private Job jobWithMetadataAndStatus(ObjectMeta objectMeta, JobStatus status) {
    return new JobBuilder().withMetadata(objectMeta).withStatus(status).build();
  }

  private ObjectMeta metadataForSegment(int index) {
    return objectMetaBuilder.withLabels(Map.of(Constants.SEGMENT_LABEL_KEY, String.valueOf(index)))
      .withName(String.format("scan-3421-%1d", index))
      .build();
  }

  private JobStatus jobStatus(int succeeded, int failed) {
    return new JobStatusBuilder().withSucceeded(succeeded).withFailed(failed).build();
  }

  private void initJobs(Set<Job> jobs) {
    when(context.getSecondaryResources(Job.class)).thenReturn(jobs);
  }

  protected void verifyResult(DuplicateMessageScanStatus status, List<Segment> expectedSegments,
                              boolean expectedSuccessful, boolean expectedFailed, String expectedError,
                              Date expectedStartTime, Date expectedUpdatedTime, Date expectedFinishedTime) {
    assertNotNull(status);
    assertEquals(expectedSegments.size(), status.segmentCount);
    assertEquals(expectedSegments, status.segments);
    assertEquals(expectedError, status.error);
    assertEquals(expectedSuccessful, status.successful);
    assertEquals(expectedFailed, status.failed);
    assertEquals(expectedSegments.stream().filter(s -> s.status == Segment.SegmentStatus.PENDING).count(),
      status.pendingSegmentCount);
    assertEquals(expectedSegments.stream().filter(s -> s.status == Segment.SegmentStatus.PROCESSING).count(),
      status.processingSegmentCount);
    assertEquals(expectedSegments.stream().filter(s -> s.status == Segment.SegmentStatus.COMPLETED).count(),
      status.completedSegmentCount);
    assertEquals(expectedSegments.stream().filter(s -> s.status == Segment.SegmentStatus.FAILED).count(),
      status.failedSegmentCount);
    assertEquals(expectedStartTime, status.scanStartedTime);
    assertEquals(expectedUpdatedTime, status.lastUpdateTime);
    assertEquals(expectedFinishedTime, status.completionTime);
  }

  private Date getClockDate() {
    return new Date(clock.millis());
  }

  static class MutableClock extends Clock {
    private final Supplier<Instant> instantSupplier;
    private final ZoneId zoneId;

    public MutableClock(Supplier<Instant> instantSupplier, ZoneId zoneId) {
      this.instantSupplier = instantSupplier;
      this.zoneId = zoneId;
    }

    @Override
    public ZoneId getZone() {
      return zoneId;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return new MutableClock(instantSupplier, zoneId);
    }

    @Override
    public Instant instant() {
      return instantSupplier.get();
    }
  }
}
