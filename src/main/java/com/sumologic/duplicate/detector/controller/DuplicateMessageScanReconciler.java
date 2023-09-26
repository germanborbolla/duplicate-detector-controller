package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanStatus;
import com.sumologic.duplicate.detector.controller.dependantresource.ConfigMapProvider;
import com.sumologic.duplicate.detector.controller.dependantresource.JobProvider;
import com.sumologic.duplicate.detector.controller.dependantresource.PersistentVolumeClaimProvider;
import com.sumologic.duplicate.detector.controller.dependantresource.ProviderKubernetesDependentResource;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Workflow;
import io.javaoperatorsdk.operator.processing.dependent.workflow.WorkflowBuilder;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;

import java.util.Map;

@ControllerConfiguration
public class DuplicateMessageScanReconciler implements Reconciler<DuplicateMessageScan>, ErrorStatusHandler<DuplicateMessageScan>, EventSourceInitializer<DuplicateMessageScan> {
  private final KubernetesDependentResource<ConfigMap, DuplicateMessageScan> configMapDependentResource;
  private final KubernetesDependentResource<PersistentVolumeClaim, DuplicateMessageScan> pvcDependentResource;
  private final KubernetesDependentResource<Job, DuplicateMessageScan> jobDependentResource;
  private final Workflow<DuplicateMessageScan> workflow;

  public DuplicateMessageScanReconciler(KubernetesClient client, ReconcilerConfiguration reconcilerConfiguration) {
    configMapDependentResource = ProviderKubernetesDependentResource.create(ConfigMap.class,
      new ConfigMapProvider(), client);
    pvcDependentResource = ProviderKubernetesDependentResource.create(PersistentVolumeClaim.class,
      new PersistentVolumeClaimProvider(reconcilerConfiguration.persistentVolumeConfiguration), client);
    jobDependentResource = ProviderKubernetesDependentResource.create(Job.class,
      new JobProvider(reconcilerConfiguration.jobConfiguration), client);

    workflow = new WorkflowBuilder<DuplicateMessageScan>()
      .addDependentResource(configMapDependentResource)
      .addDependentResource(pvcDependentResource)
      .addDependentResource(jobDependentResource).dependsOn(configMapDependentResource, pvcDependentResource).withReconcilePrecondition(
        (Condition<Job, DuplicateMessageScan>) (dependentResource, primary, context) ->
          context.getSecondaryResource(PersistentVolumeClaim.class)
            .map(pvc -> pvc.getStatus().getPhase().equals("Bound"))
            .orElse(false))
      .build();
  }

  @Override
  public UpdateControl<DuplicateMessageScan> reconcile(DuplicateMessageScan scan,
                                                       Context<DuplicateMessageScan> context) throws Exception {
    workflow.reconcile(scan, context);

    return context.getSecondaryResource(PersistentVolumeClaim.class)
      .filter(pvc -> !pvc.getStatus().getPhase().equals("Bound"))
      .map(pvc -> UpdateControl.patchStatus(scan.withStatus(
        new DuplicateMessageScanStatus("Failed to provision volume"))))
      .or(() -> context.getSecondaryResource(Job.class)
        .map(job -> UpdateControl.patchStatus(scan.withStatus(new DuplicateMessageScanStatus(job.getStatus())))))
      .orElse(UpdateControl.noUpdate());
  }

  @Override
  public ErrorStatusUpdateControl<DuplicateMessageScan> updateErrorStatus(DuplicateMessageScan resource,
                                                                          Context<DuplicateMessageScan> context,
                                                                          Exception e) {
    return ErrorStatusUpdateControl.patchStatus(resource.withStatus(new DuplicateMessageScanStatus(e)));
  }

  @Override
  public Map<String, EventSource> prepareEventSources(EventSourceContext<DuplicateMessageScan> context) {
    return EventSourceInitializer.nameEventSourcesFromDependentResource(context,
      configMapDependentResource, pvcDependentResource, jobDependentResource);
  }

}
