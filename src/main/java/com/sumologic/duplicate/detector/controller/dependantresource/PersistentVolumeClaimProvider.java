package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.*;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.Optional;
import java.util.function.Function;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class PersistentVolumeClaimProvider<R extends HasMetadata> implements DesiredProvider<PersistentVolumeClaim, R> {

    public static PersistentVolumeClaimProvider<SingleDuplicateMessageScan> createForSingleDuplicateMessageScan(ReconcilerConfiguration.PersistentVolumeConfiguration configuration) {
        return new PersistentVolumeClaimProvider<>(configuration,
          SingleDuplicateMessageScan::buildDependentObjectMetadata,
          scan -> Optional.ofNullable(scan.getSpec().getVolumeSize()));
    }

    private final ReconcilerConfiguration.PersistentVolumeConfiguration configuration;
    private final Function<R, ObjectMeta> metadataFunction;
    private final Function<R, Optional<String>> sizeExtractor;

    public PersistentVolumeClaimProvider(ReconcilerConfiguration.PersistentVolumeConfiguration configuration,
                                         Function<R, ObjectMeta> metadataFunction,
                                         Function<R, Optional<String>> sizeExtractor) {
        this.configuration = configuration;
        this.metadataFunction = metadataFunction;
        this.sizeExtractor = sizeExtractor;
    }

    @Override
    public PersistentVolumeClaim desired(R scan, Context<R> context) {
        PersistentVolumeClaim base = loadYaml(PersistentVolumeClaim.class, getClass(),
          "/baseDependantResources/volumeclaim.yaml");
        return new PersistentVolumeClaimBuilder(base)
          .withMetadata(metadataFunction.apply(scan))
          .editSpec()
          .withStorageClassName(configuration.getDefaultStorageClassName())
          .editResources().addToRequests("storage",
            Quantity.parse(sizeExtractor.apply(scan).orElse(configuration.getDefaultSize())))
          .endResources().endSpec()
          .build();
    }

}
