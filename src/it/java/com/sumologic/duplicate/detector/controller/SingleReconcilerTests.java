package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScanSpec;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class SingleReconcilerTests {

  static KubernetesClient client = new KubernetesClientBuilder().build();

  private ReconcilerConfiguration reconcilerConfiguration = new ReconcilerConfiguration(
    new ReconcilerConfiguration.JobConfiguration("busybox:latest", true, 1),
    new ReconcilerConfiguration.PersistentVolumeConfiguration("standard", "100Mi"));

  @RegisterExtension
  AbstractOperatorExtension operator = LocallyRunOperatorExtension.builder()
    .waitForNamespaceDeletion(false).preserveNamespaceOnError(true)
    .withReconciler(new SingleDuplicateMessageScanReconciler(client, reconcilerConfiguration))
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
  @DisplayName("Should not create multiple jobs")
  void oneJobPerScan() {
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
  
  private void assertScanIsComplete(String name) {
    SingleDuplicateMessageScan scan = operator.get(SingleDuplicateMessageScan.class, name);
    assertThat(scan.getStatus()).isNotNull();
    assertThat(scan.getStatus().getJobStatus()).isNotNull();
    assertThat(Optional.ofNullable(scan.getStatus().getJobStatus().getSucceeded()).orElse(0)).isEqualTo(1);
  }

  private void createScan(String name) {
    SingleDuplicateMessageScan scan = new SingleDuplicateMessageScan(new SingleDuplicateMessageScanSpec(
      "2023-09-06T10:00:00-07:00", "2023-09-06T10:01:00-07:00", "0000000000000475"));
    scan.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(operator.getNamespace())
      .build());
    operator.create(scan);
  }
}
