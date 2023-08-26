package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.Utils;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

@KubernetesDependent(labelSelector = Utils.RESOURCE_LABEL_SELECTOR)
public class PersistentVolumeClaimDependantResource extends CRUDKubernetesDependentResource<PersistentVolumeClaim, SingleDuplicateMessageScan> {
    public PersistentVolumeClaimDependantResource() {
        super(PersistentVolumeClaim.class);
    }

    @Override
    protected PersistentVolumeClaim desired(SingleDuplicateMessageScan scan, Context<SingleDuplicateMessageScan> context) {
        PersistentVolumeClaim base = loadYaml(PersistentVolumeClaim.class, getClass(),
          "/baseDependantResources/volumeclaim.yaml");
        return new PersistentVolumeClaimBuilder(base)
          .withMetadata(Utils.buildMetadata(scan))
          .editSpec().editResources()
          .addToRequests("storage", Quantity.parse(scan.getSpec().getVolumeSize().orElse("300Gi")))
          .endResources().endSpec()
          .build();
    }
}
