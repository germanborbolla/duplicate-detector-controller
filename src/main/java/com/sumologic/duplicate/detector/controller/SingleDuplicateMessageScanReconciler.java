package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanStatus;
import com.sumologic.duplicate.detector.controller.dependantresource.ConfigMapDependantResource;
import com.sumologic.duplicate.detector.controller.dependantresource.JobDependantResource;
import com.sumologic.duplicate.detector.controller.dependantresource.PersistentVolumeClaimDependantResource;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;

@ControllerConfiguration(
  dependents = {
    @Dependent(name = "cm", type = ConfigMapDependantResource.class),
    @Dependent(name = "pvc", type = PersistentVolumeClaimDependantResource.class),
    @Dependent(type = JobDependantResource.class, dependsOn = {"cm", "pvc"})
  })
public class SingleDuplicateMessageScanReconciler implements Reconciler<SingleDuplicateMessageScan>, ErrorStatusHandler<SingleDuplicateMessageScan> {
  @Override
  public UpdateControl<SingleDuplicateMessageScan> reconcile(SingleDuplicateMessageScan scan,
                                                             Context<SingleDuplicateMessageScan> context) throws Exception {
    return UpdateControl.patchStatus(scan.withStatus(
      new DuplicateMessageScanStatus(context.getSecondaryResource(Job.class).orElseThrow().getStatus())));
  }

  @Override
  public ErrorStatusUpdateControl<SingleDuplicateMessageScan> updateErrorStatus(SingleDuplicateMessageScan resource,
                                                                                Context<SingleDuplicateMessageScan> context,
                                                                                Exception e) {
    return ErrorStatusUpdateControl.patchStatus(resource.withStatus(new DuplicateMessageScanStatus(e)));
  }
}
