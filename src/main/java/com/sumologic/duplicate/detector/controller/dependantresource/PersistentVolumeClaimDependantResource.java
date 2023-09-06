package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.Constants;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import java.util.Optional;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

@KubernetesDependent(labelSelector = Constants.RESOURCE_LABEL_SELECTOR)
public class PersistentVolumeClaimDependantResource extends CRUDKubernetesDependentResource<PersistentVolumeClaim, SingleDuplicateMessageScan> {
    private static final String STORAGE_CLASS_NAME = Optional.ofNullable(System.getenv("PVC_STORAGE_CLASS_NAME"))
      .orElse("gp2");
    private static final String DEFAULT_SIZE = Optional.ofNullable(System.getenv("PVC_DEFAULT_SIZE")).
      orElse("300Gi");
    public PersistentVolumeClaimDependantResource() {
        super(PersistentVolumeClaim.class);
    }

    @Override
    protected PersistentVolumeClaim desired(SingleDuplicateMessageScan scan, Context<SingleDuplicateMessageScan> context) {
        PersistentVolumeClaim base = loadYaml(PersistentVolumeClaim.class, getClass(),
          "/baseDependantResources/volumeclaim.yaml");
        String size = DEFAULT_SIZE;
        if (scan.getSpec().getVolumeSize() != null) {
            size = scan.getSpec().getVolumeSize();
        }
        return new PersistentVolumeClaimBuilder(base)
          .withMetadata(scan.buildDependentObjectMetadata())
          .editSpec()
          .withStorageClassName(STORAGE_CLASS_NAME)
          .editResources().addToRequests("storage", Quantity.parse(size)).endResources()
          .endSpec()
          .build();
    }
}
