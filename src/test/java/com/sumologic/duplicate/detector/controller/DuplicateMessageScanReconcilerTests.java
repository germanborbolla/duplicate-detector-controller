package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanSpec;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanStatus;
import com.sumologic.duplicate.detector.controller.customresource.Segment;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DuplicateMessageScanReconcilerTests {

  @Mock
  private KubernetesClient client;
  @Mock
  private Context<DuplicateMessageScan> context;

  private DuplicateMessageScanReconciler reconciler;

  private DuplicateMessageScanSpec spec = new DuplicateMessageScanSpec("2023-09-06T10:00:00-07:00",
    "2023-09-06T10:15:00-07:00", List.of("0000000000000005"));
  private DuplicateMessageScan scan = new DuplicateMessageScan(spec);

  private ReconcilerConfiguration configuration = new ReconcilerConfiguration(
    new ReconcilerConfiguration.JobConfiguration("image:123", true,
      5, true),
    new ReconcilerConfiguration.PersistentVolumeConfiguration("gp2", "100Mi"));

  @Test
  void testSingleNoJob() {
    when(context.getSecondaryResource(Job.class)).thenReturn(Optional.empty());

    testSingleReconcile(spec.getSegments(), false, false, null);
  }

  @Test
  void testSingleJobNoStatus() {
    initJob(null);

    testSingleReconcile(segmentsWithStatus(Segment.SegmentStatus.PROCESSING), false, false,
      null);
  }

  @Test
  void testSingleJobNotCompleted() {
    initJobWithStatus(0, 0);

    testSingleReconcile(segmentsWithStatus(Segment.SegmentStatus.PROCESSING), false, false,
      null);
  }

  @Test
  void testSingleJobFailed() {
    initJobWithStatus(0, 1);

    testSingleReconcile(segmentsWithStatus(Segment.SegmentStatus.FAILED), false, true,
      "All attempts to execute scan failed");
  }

  @Test
  void testSingleJobSucceeded() {
    initJobWithStatus(1, 0);

    testSingleReconcile(segmentsWithStatus(Segment.SegmentStatus.COMPLETED), true, false, null);
  }

  @Test
  void testSingleWithNullSucceeded() {
    JobStatus status = new JobStatus();
    initJob(status);

    testSingleReconcile(segmentsWithStatus(Segment.SegmentStatus.PROCESSING), false, false,
      null);
  }

  @BeforeEach
  void beforeEach() {
    reconciler = new DuplicateMessageScanReconciler(client, configuration);

    scan = new DuplicateMessageScan(spec);
  }

  private void testSingleReconcile(List<Segment> expectedSegments,
                                   boolean expectedSuccessful, boolean expectedFailed, String expectedError) {
    UpdateControl<DuplicateMessageScan> control = reconciler.singleReconcile(scan, context, false);
    DuplicateMessageScanStatus status = control.getResource().getStatus();
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
  }

  private List<Segment> segmentsWithStatus(Segment.SegmentStatus status) {
    return spec.getSegments().stream().peek(s -> s.status = status).collect(Collectors.toList());
  }

  private void initJobWithStatus(int succeeded, int failed) {
    JobStatus jobStatus = new JobStatus();
    jobStatus.setSucceeded(succeeded);
    jobStatus.setFailed(failed);
    initJob(jobStatus);
  }
  private void initJob(JobStatus status) {
    Job job = new Job();
    job.setStatus(status);
    when(context.getSecondaryResource(Job.class)).thenReturn(Optional.of(job));
  }
}
