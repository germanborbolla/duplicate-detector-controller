package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class PersistentVolumeClaimProvider implements DesiredProvider<PersistentVolumeClaim, SingleDuplicateMessageScan> {
    private ReconcilerConfiguration.PersistentVolumeConfiguration configuration;

    public PersistentVolumeClaimProvider(ReconcilerConfiguration.PersistentVolumeConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public PersistentVolumeClaim desired(SingleDuplicateMessageScan scan, Context<SingleDuplicateMessageScan> context) {
        PersistentVolumeClaim base = loadYaml(PersistentVolumeClaim.class, getClass(),
          "/baseDependantResources/volumeclaim.yaml");
        String size = configuration.getDefaultSize();
        if (scan.getSpec().getVolumeSize() != null) {
            size = scan.getSpec().getVolumeSize();
        }
        return new PersistentVolumeClaimBuilder(base)
          .withMetadata(scan.buildDependentObjectMetadata())
          .editSpec()
          .withStorageClassName(configuration.getDefaultStorageClassName())
          .editResources().addToRequests("storage", Quantity.parse(size)).endResources()
          .endSpec()
          .build();
    }
}
