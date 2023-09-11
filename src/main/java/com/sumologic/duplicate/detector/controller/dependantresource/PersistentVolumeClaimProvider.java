package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.Optional;

public class PersistentVolumeClaimProvider extends BasePersistentVolumeClaimProvider implements DesiredProvider<PersistentVolumeClaim, SingleDuplicateMessageScan> {

    public PersistentVolumeClaimProvider(ReconcilerConfiguration.PersistentVolumeConfiguration configuration) {
        super(configuration);
    }

    @Override
    public PersistentVolumeClaim desired(SingleDuplicateMessageScan scan, Context<SingleDuplicateMessageScan> context) {
        return desiredPersistentVolumeClaim(Optional.ofNullable(scan.getSpec().getVolumeSize()),
          scan.buildDependentObjectMetadata());
    }

}
