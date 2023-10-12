package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.Constants;
import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.Segment;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResourceConfig;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class PersistentVolumeClaimDependentResource extends AbstractBulkDependentResource<PersistentVolumeClaim> {
  public static PersistentVolumeClaimDependentResource create(ReconcilerConfiguration.PersistentVolumeConfiguration configuration, KubernetesClient client) {
    PersistentVolumeClaimDependentResource resource = new PersistentVolumeClaimDependentResource(configuration);
    resource.setKubernetesClient(client);
    resource.configureWith(new KubernetesDependentResourceConfig<PersistentVolumeClaim>()
      .setLabelSelector(Constants.RESOURCE_LABEL_SELECTOR));
    return resource;
  }
  private final ReconcilerConfiguration.PersistentVolumeConfiguration configuration;

  public PersistentVolumeClaimDependentResource(ReconcilerConfiguration.PersistentVolumeConfiguration configuration) {
    super(PersistentVolumeClaim.class);
    this.configuration = configuration;
  }

  @Override
  public Map<String, PersistentVolumeClaim> desiredResources(DuplicateMessageScan scan,
                                                             Context<DuplicateMessageScan> context) {
    PersistentVolumeClaimBuilder pvcBuilder = new PersistentVolumeClaimBuilder(
      loadYaml(PersistentVolumeClaim.class, getClass(), "/baseDependantResources/volumeclaim.yaml"))
      .editSpec()
      .withStorageClassName(configuration.getDefaultStorageClassName())
      .editResources().addToRequests("storage",
        Quantity.parse(Optional.ofNullable(scan.getSpec().volumeSize).orElse(configuration.getDefaultSize())))
      .endResources().endSpec();
    Stream<Segment> segments;
    if (scan.getStatus() != null) {
      segments = scan.getStatus().segments.stream().filter(Segment::isPendingOrProcessing);
    } else {
      segments = scan.getSpec().getSegments().stream();
    }
    return segments.collect(Collectors.toMap(
      s -> s.id,
      s -> pvcBuilder.withMetadata(new ObjectMetaBuilder(scan.buildDependentObjectMetadata())
        .withName(String.format("%s-%s", scan.getMetadata().getName(), s.id))
        .addToLabels(Constants.SEGMENT_LABEL_KEY, s.id)
        .build())
        .build()
    ));
  }
}
