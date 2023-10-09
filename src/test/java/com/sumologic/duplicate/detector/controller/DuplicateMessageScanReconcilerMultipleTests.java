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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.when;

public class DuplicateMessageScanReconcilerMultipleTests extends DuplicateMessageScanReconcilerTestBase {

  private DuplicateMessageScanSpec spec = new DuplicateMessageScanSpec("2023-09-06T10:00:00-07:00",
    "2023-09-06T10:15:00-07:00", List.of("0000000000000005","0000000000000006"));

  private DuplicateMessageScan scan = new DuplicateMessageScan(spec);
  private ObjectMetaBuilder objectMetaBuilder = new ObjectMetaBuilder().withName("scan-3421-0");

  @Test
  void testNoJobs() {
    initJobs(Set.of());

    testReconcile(spec.getSegments(), false, false, null);
  }

  @Test
  void testJobWithoutLabels() {
    initJobs(Set.of(jobWithMetadata(objectMetaBuilder.build())));

    testReconcile(spec.getSegments(), false, false, null);
  }

  @Test
  void testJobForSegmentExistsNoStatus() {
    initJobs(Set.of(jobWithMetadata(metadataForSegment(0))));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.get(0).processing();
    testReconcile(segments, false, false, null);
  }

  @Test
  void testJobForSegmentEmptyStatus() {
    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(0), new JobStatus())));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.get(0).processing();
    testReconcile(segments, false, false, null);
  }

  @Test
  void testPendingJobForSegment() {
    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(0), jobStatus(0, 0))));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.get(0).processing();
    testReconcile(segments, false, false, null);
  }

  @Test
  void testSucceededJobForSegment() {
    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(0), jobStatus(1, 0))));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.get(0).completed();
    testReconcile(segments, false, false, null);
  }

  @Test
  void testFailedJobForSegment() {
    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(0), jobStatus(0, 1))));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.get(0).failed();
    testReconcile(segments, false, false, null);
  }

  @Test
  void allSegmentsSucceeded() {
    DuplicateMessageScanStatus status = new DuplicateMessageScanStatus(spec.getSegments());
    status.segments.get(0).completed();
    scan.setStatus(status);

    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(1), jobStatus(1, 0))));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.forEach(Segment::completed);
    testReconcile(segments, true, false, null);
  }

  @Test
  void allSegmentsFinishedWithSomeFailures() {
    DuplicateMessageScanStatus status = new DuplicateMessageScanStatus(spec.getSegments());
    status.segments.get(0).failed();
    scan.setStatus(status);

    initJobs(Set.of(jobWithMetadataAndStatus(metadataForSegment(1), jobStatus(1, 0))));

    List<Segment> segments = List.copyOf(spec.getSegments());
    segments.get(0).failed();
    segments.get(1).completed();
    testReconcile(segments, false, true, "One or more jobs failed");
  }

  @BeforeEach
  protected void beforeEach() {
    super.beforeEach();
    scan = new DuplicateMessageScan(spec);
  }

  private void testReconcile(List<Segment> expectedSegments,
                             boolean expectedSuccessful, boolean expectedFailed, String expectedError) {
    verifyResult(reconciler.calculateStatus(scan, context),
      expectedSegments, expectedSuccessful, expectedFailed, expectedError);
  }

  private Job jobWithMetadata(ObjectMeta objectMeta) {
    return new JobBuilder().withMetadata(objectMeta).build();
  }

  private Job jobWithMetadataAndStatus(ObjectMeta objectMeta, JobStatus status) {
    return new JobBuilder().withMetadata(objectMeta).withStatus(status).build();
  }

  private ObjectMeta metadataForSegment(int index) {
    return objectMetaBuilder.withLabels(Map.of(Constants.JOB_SEGMENT_LABEL_KEY, String.valueOf(index)))
      .withName(String.format("scan-3421-%1d", index))
      .build();
  }

  private JobStatus jobStatus(int succeeded, int failed) {
    return new JobStatusBuilder().withSucceeded(succeeded).withFailed(failed).build();
  }

  private void initJobs(Set<Job> jobs) {
    when(context.getSecondaryResources(Job.class)).thenReturn(jobs);
  }
}
