package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.Optional;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class PersistentVolumeClaimProvider implements DesiredProvider<PersistentVolumeClaim, DuplicateMessageScan> {
    private final ReconcilerConfiguration.PersistentVolumeConfiguration configuration;
    public PersistentVolumeClaimProvider(ReconcilerConfiguration.PersistentVolumeConfiguration configuration) {
        this.configuration = configuration;
    }
    @Override
    public PersistentVolumeClaim desired(DuplicateMessageScan scan, Context<DuplicateMessageScan> context) {
        PersistentVolumeClaim base = loadYaml(PersistentVolumeClaim.class, getClass(),
          "/baseDependantResources/volumeclaim.yaml");
        return new PersistentVolumeClaimBuilder(base)
          .withMetadata(scan.buildDependentObjectMetadata())
          .editSpec()
          .withStorageClassName(configuration.getDefaultStorageClassName())
          .editResources().addToRequests("storage",
            Quantity.parse(Optional.ofNullable(scan.getSpec().getVolumeSize()).orElse(configuration.getDefaultSize())))
          .endResources().endSpec()
          .build();
    }

}
