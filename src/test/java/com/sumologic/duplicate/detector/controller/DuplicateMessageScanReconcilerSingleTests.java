package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanSpec;
import com.sumologic.duplicate.detector.controller.customresource.Segment;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;

public class DuplicateMessageScanReconcilerSingleTests extends DuplicateMessageScanReconcilerTestBase {

  private DuplicateMessageScanSpec spec = new DuplicateMessageScanSpec("2023-09-06T10:00:00-07:00",
    "2023-09-06T10:15:00-07:00", List.of("0000000000000005"));
  private DuplicateMessageScan scan = new DuplicateMessageScan(spec);

  @Test
  void testNoJob() {
    when(context.getSecondaryResource(Job.class)).thenReturn(Optional.empty());

    testReconcile(spec.getSegments(), false, false, null);
  }

  @Test
  void testJobNoStatus() {
    initJob(null);

    testReconcile(segmentsWithStatus(Segment.SegmentStatus.PROCESSING), false, false,
      null);
  }

  @Test
  void testJobNotCompleted() {
    initJobWithStatus(0, 0);

    testReconcile(segmentsWithStatus(Segment.SegmentStatus.PROCESSING), false, false,
      null);
  }

  @Test
  void testJobFailed() {
    initJobWithStatus(0, 1);

    testReconcile(segmentsWithStatus(Segment.SegmentStatus.FAILED), false, true,
      "All attempts to execute scan failed");
  }

  @Test
  void testJobSucceeded() {
    initJobWithStatus(1, 0);

    testReconcile(segmentsWithStatus(Segment.SegmentStatus.COMPLETED), true, false, null);
  }

  @Test
  void testWithNullSucceeded() {
    JobStatus status = new JobStatus();
    initJob(status);

    testReconcile(segmentsWithStatus(Segment.SegmentStatus.PROCESSING), false, false,
      null);
  }

  @BeforeEach
  protected void beforeEach() {
    super.beforeEach();
    scan = new DuplicateMessageScan(spec);
  }

  private void testReconcile(List<Segment> expectedSegments,
                                   boolean expectedSuccessful, boolean expectedFailed, String expectedError) {
    UpdateControl<DuplicateMessageScan> control = reconciler.singleReconcile(scan, context, false);
    verifyResult(control, expectedSegments, expectedSuccessful, expectedFailed, expectedError);
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
