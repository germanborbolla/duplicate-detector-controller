package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;

import java.util.Optional;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class BasePersistentVolumeClaimProvider {
  protected ReconcilerConfiguration.PersistentVolumeConfiguration configuration;

  public BasePersistentVolumeClaimProvider(ReconcilerConfiguration.PersistentVolumeConfiguration configuration) {
    this.configuration = configuration;
  }

  protected PersistentVolumeClaim desiredPersistentVolumeClaim(Optional<String> volumeSize, ObjectMeta objectMeta) {
    PersistentVolumeClaim base = loadYaml(PersistentVolumeClaim.class, getClass(),
      "/baseDependantResources/volumeclaim.yaml");
    return new PersistentVolumeClaimBuilder(base)
      .withMetadata(objectMeta)
      .editSpec()
      .withStorageClassName(configuration.getDefaultStorageClassName())
      .editResources().addToRequests("storage", Quantity.parse(volumeSize.orElse(configuration.getDefaultSize())))
      .endResources().endSpec()
      .build();
  }
}
