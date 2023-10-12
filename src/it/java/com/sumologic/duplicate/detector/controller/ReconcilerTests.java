package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanSpec;
import com.sumologic.duplicate.detector.controller.customresource.Segment;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.javaoperatorsdk.operator.junit.AbstractOperatorExtension;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    ResourceWatcher<Job> jobWatcher = new ResourceWatcher<>();
    ResourceWatcher<PersistentVolumeClaim> pvcWatcher = new ResourceWatcher<>();
    operator.getKubernetesClient().resources(Job.class).inNamespace(operator.getNamespace()).watch(jobWatcher);
    operator.getKubernetesClient().resources(PersistentVolumeClaim.class).inNamespace(operator.getNamespace()).watch(pvcWatcher);
    await()
      .atMost(Duration.ofMinutes(1))
      .pollInterval(Duration.ofSeconds(1))
      .untilAsserted(
        () -> {

          ConfigMap configMap = operator.get(ConfigMap.class, name);
          assertThat(configMap).isNotNull();
          assertThat(configMap.getMetadata().getName()).isEqualTo(name);

          assertScanIsComplete(name);

          PersistentVolumeClaim pvc = pvcWatcher.resources.get("0");
          assertThat(pvc).isNotNull();
          assertThat(pvcWatcher.actionsPerJob.get("0")).contains(Watcher.Action.DELETED);

          Job job = jobWatcher.resources.get("0");
          assertThat(job).isNotNull();
          assertThat(job.getStatus().getSucceeded()).isEqualTo(1);
          assertThat(job.getStatus().getCompletionTime()).isNotNull();
          assertThat(jobWatcher.actionsPerJob.get("0")).contains(Watcher.Action.DELETED);
        });
  }

  @Test
  @DisplayName("Should not create multiple jobs for simple jobs")
  void oneJobPerSimpleScan() {
    String name = "test-scan";
    createScan(name);

    ResourceWatcher<Job> watcher = new ResourceWatcher<>();
    operator.getKubernetesClient().resources(Job.class).inNamespace(operator.getNamespace()).watch(watcher);
    await()
      .atMost(Duration.ofMinutes(1))
      .pollInterval(Duration.ofSeconds(1))
      .untilAsserted(() -> assertScanIsComplete(name));

    await()
      .during(Duration.ofSeconds(15))
      .atMost(Duration.ofSeconds(30))
      .pollInterval(Duration.ofSeconds(1))
      .until(watcher.resources::size, size -> size == 1);
  }

  @Test
  @DisplayName("schedule multiple jobs for parallel scans")
  void noVolumeWhenParallelGreaterThanOne() {
    String name = "test-scan";
    spec.customers = List.of("0000000000000475", "0000000000000476");
    createScan(name);
    ResourceWatcher<Job> jobWatcher = new ResourceWatcher<>();
    ResourceWatcher<PersistentVolumeClaim> pvcWatcher = new ResourceWatcher<>();
    operator.getKubernetesClient().resources(Job.class).inNamespace(operator.getNamespace()).watch(jobWatcher);
    operator.getKubernetesClient().resources(PersistentVolumeClaim.class).inNamespace(operator.getNamespace()).watch(pvcWatcher);
    await()
      .atMost(Duration.ofMinutes(1))
      .pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
        PersistentVolumeClaim pvc = operator.get(PersistentVolumeClaim.class, name);
        assertThat(pvc).isNull();

        assertScanIsComplete(name);

        assertThat(jobWatcher.resources).hasSize(spec.getSegments().size());
        assertThat(jobWatcher.resources.keySet()).containsAll(spec.getSegments().stream().map(s -> s.id).collect(Collectors.toList()));
        assertThat(jobWatcher.resources.values()).allMatch(job -> job.getStatus().getSucceeded() == 1);
        spec.getSegments().forEach(s -> assertThat(jobWatcher.actionsPerJob.get(s.id)).withFailMessage(String.format("Job %s was not deleted", s.id)).contains(Watcher.Action.DELETED));

        assertThat(pvcWatcher.resources).hasSize(spec.getSegments().size());
        assertThat(pvcWatcher.resources.keySet()).containsAll(spec.getSegments().stream().map(s -> s.id).collect(Collectors.toList()));
        spec.getSegments().forEach(s -> assertThat(pvcWatcher.actionsPerJob.get(s.id)).withFailMessage(String.format("PVC %s was not deleted", s.id)).contains(Watcher.Action.DELETED));
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

  static class ResourceWatcher<R extends HasMetadata> implements Watcher<R> {
    Map<String, R> resources = new HashMap<>();
    Map<String, List<Watcher.Action>> actionsPerJob = new HashMap<>();
    @Override
    public void eventReceived(Action action, R resource) {
      String key = resource.getMetadata().getLabels().get(Constants.SEGMENT_LABEL_KEY);
      resources.put(key, resource);
      actionsPerJob.computeIfAbsent(key, k -> new LinkedList<>()).add(action);
    }

    @Override
    public void onClose(WatcherException cause) {

    }
  }
}
