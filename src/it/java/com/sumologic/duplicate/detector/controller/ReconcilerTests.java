package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanSpec;
import com.sumologic.duplicate.detector.controller.customresource.Segment;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.javaoperatorsdk.operator.junit.AbstractOperatorExtension;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertWith;
import static org.awaitility.Awaitility.await;

public class ReconcilerTests {

  static KubernetesClient client = new KubernetesClientBuilder().build();

  private DuplicateMessageScanSpec spec = new DuplicateMessageScanSpec(
    "2023-09-06T10:00:00-07:00", "2023-09-06T10:01:00-07:00",
    List.of("0000000000000475"));

  private ReconcilerConfiguration reconcilerConfiguration = new ReconcilerConfiguration(
    new ReconcilerConfiguration.JobConfiguration("busybox:latest", true, 1, true),
    new ReconcilerConfiguration.PersistentVolumeConfiguration("standard", "100Mi"));

  @RegisterExtension
  AbstractOperatorExtension operator = LocallyRunOperatorExtension.builder()
    .waitForNamespaceDeletion(false).preserveNamespaceOnError(true)
    .withReconciler(new DuplicateMessageScanReconciler(client, reconcilerConfiguration))
    .build();

  @Test
  @DisplayName("Generate the resources for a scan")
  void generate() {
    String name = "test-scan";
    createScan(name);

    await()
      .atMost(Duration.ofMinutes(1))
      .pollInterval(Duration.ofSeconds(1))
      .untilAsserted(
        () -> {

          ConfigMap configMap = operator.get(ConfigMap.class, name);
          assertThat(configMap).isNotNull();
          assertThat(configMap.getMetadata().getName()).isEqualTo(name);

          PersistentVolumeClaim pvc = operator.get(PersistentVolumeClaim.class, name);
          assertThat(pvc).isNotNull();

          Job job = operator.get(Job.class, name);
          assertThat(job).isNotNull();
          assertThat(job.getStatus().getSucceeded()).isEqualTo(1);
          assertThat(job.getStatus().getCompletionTime()).isNotNull();

          assertScanIsComplete(name);
        });
  }

  @Test
  @DisplayName("Should not create multiple jobs for simple jobs")
  void oneJobPerSimpleScan() {
    String name = "test-scan";
    createScan(name);

    List<Job> jobs = new ArrayList<>(1);
    operator.getKubernetesClient().resources(Job.class).inNamespace(operator.getNamespace()).watch(new Watcher<>() {
      @Override
      public void eventReceived(Action action, Job resource) {
        if (action.equals(Action.ADDED)) {
          jobs.add(resource);
        }
      }

      @Override
      public void onClose(WatcherException cause) {

      }
    });
    await()
      .atMost(Duration.ofMinutes(1))
      .pollInterval(Duration.ofSeconds(1))
      .untilAsserted(() -> assertScanIsComplete(name));

    await()
      .during(Duration.ofSeconds(15))
      .atMost(Duration.ofSeconds(30))
      .pollInterval(Duration.ofSeconds(1))
      .until(jobs::size, size -> size == 1);
  }

  @Test
  @DisplayName("Not create a volume if parallelism is greater than one")
  @Disabled("While I rework parallel logic")
  void noVolumeWhenParallelGreaterThanOne() {
    String name = "test-scan";
    spec.maxParallelScans = 2;
    createScan(name);
    await()
      .atMost(Duration.ofMinutes(1))
      .pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
        PersistentVolumeClaim pvc = operator.get(PersistentVolumeClaim.class, name);
        assertThat(pvc).isNull();

        Job job = operator.get(Job.class, name);
        assertThat(job).isNotNull();
        assertThat(job.getStatus().getSucceeded()).isEqualTo(2);
        assertThat(job.getStatus().getCompletionTime()).isNotNull();

        assertScanIsComplete(name);
      });
  }
  private void assertScanIsComplete(String name) {
    DuplicateMessageScan scan = operator.get(DuplicateMessageScan.class, name);
    List<Segment> expectedSegments = scan.getSpec().getSegments();
    assertThat(scan.getStatus()).isNotNull();
    assertWith(scan.getStatus(), status -> {
      assertThat(status).isNotNull();
      assertThat(status.successful).isTrue();
      assertThat(status.failed).isFalse();
      assertThat(status.error).isNull();
      assertThat(status.segments).hasSize(expectedSegments.size());
      assertThat(status.segments).allMatch(s -> s.status == Segment.SegmentStatus.COMPLETED);
    });
  }

  private void createScan(String name) {
    DuplicateMessageScan scan = new DuplicateMessageScan(spec);
    scan.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(operator.getNamespace())
      .build());
    operator.create(scan);
  }
}
