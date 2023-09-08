package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScanSpec;
import com.sumologic.duplicate.detector.controller.dependantresource.BaseTests;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.javaoperatorsdk.operator.junit.AbstractOperatorExtension;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class SingleDuplicateMessageScanReconcilerTests extends BaseTests {

  static KubernetesClient client = new KubernetesClientBuilder().build();

  private ReconcilerConfiguration reconcilerConfiguration = new ReconcilerConfiguration(
    new ReconcilerConfiguration.JobConfiguration("busybox:latest", true, 5),
    new ReconcilerConfiguration.PersistentVolumeConfiguration("standard", "100Mi"));

  @RegisterExtension
  AbstractOperatorExtension operator = LocallyRunOperatorExtension.builder()
    .withAdditionalCustomResourceDefinition(SingleDuplicateMessageScan.class)
    .waitForNamespaceDeletion(false).preserveNamespaceOnError(true)
    .withReconciler(new SingleDuplicateMessageScanReconciler(client, reconcilerConfiguration))
    .build();

  @Test
  @DisplayName("Generate the resources for a scan")
  void generateResources() {
    String name = "test-scan";
    SingleDuplicateMessageScan scan = new SingleDuplicateMessageScan(new SingleDuplicateMessageScanSpec(
      "2023-09-06T10:00:00-07:00", "2023-09-06T10:01:00-07:00", "0000000000000475"));
    scan.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(operator.getNamespace())
      .build());
    operator.create(scan);

    await()
      .atMost(Duration.ofMinutes(2))
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

          SingleDuplicateMessageScan actualScan = operator.get(SingleDuplicateMessageScan.class, name);
          assertThat(actualScan.getStatus()).isNotNull();
          assertThat(actualScan.getStatus().getJobStatus()).isEqualTo(operator.get(Job.class, name).getStatus());
        });
  }
}
