package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.Constants;
import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanSpec;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class JobDependentResourceTests extends BaseTests {

  private ReconcilerConfiguration.JobConfiguration configuration = new ReconcilerConfiguration.JobConfiguration(
    "system-tools:123", false, 5, true);

  @Mock
  private Context<DuplicateMessageScan> context;

  private JobDependentResource resource;

  private DuplicateMessageScanSpec spec = new DuplicateMessageScanSpec("2023-09-06T10:00:00-07:00",
    "2023-09-06T10:15:00-07:00", List.of("0000000000000005", "0000000000000006"));
  private DuplicateMessageScan scan;

  @Nested
  class GetSecondaryResources {
    @Test
    void returnEmptyWhenNoJobsExist() {
      initJobs(Stream.empty());

      assertEquals(Map.of(), resource.getSecondaryResources(scan, context));
    }

    @Test
    void ignoreJobsWithNoLabels() {
      initJobs(Stream.of(
        new Job(),
        new JobBuilder().withMetadata(new ObjectMeta()).build(),
        new JobBuilder().withMetadata(
          new ObjectMetaBuilder().withLabels(Map.of("somelabel", "value")).build()).build()));

      assertEquals(Map.of(), resource.getSecondaryResources(scan, context));
    }

    @Test
    void mapJobsByTheirLabel() {
      Job job0 = jobForSegment(0);
      Job job1 = jobForSegment(1);
      initJobs(Stream.of(job0, job1));

      assertEquals(Map.of("0", job0, "1", job1), resource.getSecondaryResources(scan, context));
    }
  }

  @Nested
  class DesiredResources {
    @Test
    void handleNonParallelScans() {
      Map<String, Job> desiredJobs = resource.desiredResources(scan, context);

      verifyJobs(Map.ofEntries(entryForSegment(0)), desiredJobs);
      DuplicateMessageScanStatus status = new DuplicateMessageScanStatus(spec.getSegments());
      scan.setStatus(status);

      verifyJobs(Map.ofEntries(entryForSegment(0)), resource.desiredResources(scan, context));

      status.segments.get(0).processing();

      verifyJobs(Map.ofEntries(entryForSegment(0)), resource.desiredResources(scan, context));

      status.segments.get(0).completed();

      verifyJobs(Map.ofEntries(entryForSegment(1)), resource.desiredResources(scan, context));

      status.completed();

      verifyJobs(Map.of(), resource.desiredResources(scan, context));
    }

    @Test
    void handleParallelScans() {
      spec.maxParallelScans = 2;
      spec.timeRangeSegmentLength = "PT5m";

      verifyJobs(Map.ofEntries(entryForSegment(0), entryForSegment(1)),
        resource.desiredResources(scan, context));

      DuplicateMessageScanStatus status = new DuplicateMessageScanStatus(spec.getSegments());
      scan.setStatus(status);

      verifyJobs(Map.ofEntries(entryForSegment(0), entryForSegment(1)),
        resource.desiredResources(scan, context));

      status.segments.get(0).completed();

      verifyJobs(Map.ofEntries(entryForSegment(1), entryForSegment(2)),
        resource.desiredResources(scan, context));

      status.completed();

      verifyJobs(Map.of(), resource.desiredResources(scan, context));
    }

    @Test
    void handleSingleSegmentScans() {
      spec.customers = List.of("0000000000000005");
      verifyJobs(Map.of("0", jobForSegment("0")),
        resource.desiredResources(scan, context));

      DuplicateMessageScanStatus status = new DuplicateMessageScanStatus(spec.getSegments());
      scan.setStatus(status);
      status.completed();

      verifyJobs(Map.of(), resource.desiredResources(scan, context));
    }
  }
  @BeforeEach
  void beforeEach() {
    resource = new JobDependentResource(configuration);

    scan = new DuplicateMessageScan(spec);
    scan.setMetadata(objectMeta);
  }

  private void verifyJobs(Map<String, Job> expected, Map<String, Job> actual) {
    assertEquals(expected.size(), actual.size());
    expected.forEach((key, job) -> {
      assertTrue(actual.containsKey(key), String.format("Job for key %1s not found, keys are: %1s", key,
        actual.keySet()));
      assertEqualsWithYaml(job, actual.get(key));
    });
  }

  private void initJobs(Stream<Job> jobs) {
    when(context.getSecondaryResourcesAsStream(Job.class)).thenReturn(jobs);
  }

  private Map.Entry<String, Job> entryForSegment(int index) {
    return Map.entry(String.valueOf(index), jobForSegment(index));
  }
  private Job jobForSegment(String segmentIndex) {
    Job baseExpectedJob = loadYaml(Job.class, getClass(), "/job/base-job.yaml");
    String name = String.format("test-%1s", segmentIndex);
    return new JobBuilder(baseExpectedJob)
      .withMetadata(new ObjectMetaBuilder(baseExpectedJob.getMetadata())
        .withName(name)
        .addToLabels(Constants.SEGMENT_LABEL_KEY, segmentIndex).build())
      .editSpec().editTemplate().editSpec()
      .editFirstContainer()
      .addNewEnv().withName(Constants.JOB_COMPLETION_INDEX_ENV_NAME).withValue(segmentIndex).endEnv()
      .endContainer()
      .editMatchingVolume(v -> v.getName().equals("duplicate-detector-pvc"))
      .editPersistentVolumeClaim().withClaimName(name).endPersistentVolumeClaim()
      .endVolume()
      .endSpec().endTemplate().endSpec()
      .build();
  }


  private Job jobForSegment(int index) {
    return jobForSegment(String.valueOf(index));
  }
}
