package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanStatus;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.dependantresource.ConfigMapProvider;
import com.sumologic.duplicate.detector.controller.dependantresource.JobProvider;
import com.sumologic.duplicate.detector.controller.dependantresource.PersistentVolumeClaimProvider;
import com.sumologic.duplicate.detector.controller.dependantresource.ProviderKubernetesDependentResource;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Workflow;
import io.javaoperatorsdk.operator.processing.dependent.workflow.WorkflowBuilder;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

@ControllerConfiguration
public class SingleDuplicateMessageScanReconciler implements Reconciler<SingleDuplicateMessageScan>, ErrorStatusHandler<SingleDuplicateMessageScan>, EventSourceInitializer<SingleDuplicateMessageScan> {

  private final Logger logger = LoggerFactory.getLogger(SingleDuplicateMessageScanReconciler.class);
  private final KubernetesDependentResource<ConfigMap, SingleDuplicateMessageScan> configMapDependentResource;
  private final KubernetesDependentResource<PersistentVolumeClaim, SingleDuplicateMessageScan> pvcDependentResource;
  private final KubernetesDependentResource<Job, SingleDuplicateMessageScan> jobDependentResource;
  private final Workflow<SingleDuplicateMessageScan> workflow;

  public SingleDuplicateMessageScanReconciler(KubernetesClient client, ReconcilerConfiguration reconcilerConfiguration) {
    configMapDependentResource = ProviderKubernetesDependentResource.create(ConfigMap.class,
      ConfigMapProvider.createForSingleDuplicateMessageScan(), client);
    pvcDependentResource = ProviderKubernetesDependentResource.create(PersistentVolumeClaim.class,
      PersistentVolumeClaimProvider.createForSingleDuplicateMessageScan(
        reconcilerConfiguration.persistentVolumeConfiguration), client);
    jobDependentResource = ProviderKubernetesDependentResource.create(Job.class,
      JobProvider.createForSingleDuplicateMessageScan(reconcilerConfiguration.jobConfiguration), client);

    workflow = new WorkflowBuilder<SingleDuplicateMessageScan>()
      .addDependentResource(configMapDependentResource)
      .addDependentResource(pvcDependentResource)
      .addDependentResource(jobDependentResource).dependsOn(configMapDependentResource, pvcDependentResource).withReconcilePrecondition(
        (Condition<Job, SingleDuplicateMessageScan>) (dependentResource, primary, context) ->
          context.getSecondaryResource(PersistentVolumeClaim.class)
            .map(pvc -> pvc.getStatus().getPhase().equals("Bound"))
            .orElse(false))
      .build();
  }

  @Override
  public UpdateControl<SingleDuplicateMessageScan> reconcile(SingleDuplicateMessageScan scan,
                                                             Context<SingleDuplicateMessageScan> context) throws Exception {
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
  public ErrorStatusUpdateControl<SingleDuplicateMessageScan> updateErrorStatus(SingleDuplicateMessageScan resource,
                                                                                Context<SingleDuplicateMessageScan> context,
                                                                                Exception e) {
    return ErrorStatusUpdateControl.patchStatus(resource.withStatus(new DuplicateMessageScanStatus(e)));
  }

  @Override
  public Map<String, EventSource> prepareEventSources(EventSourceContext<SingleDuplicateMessageScan> context) {
    return EventSourceInitializer.nameEventSourcesFromDependentResource(context,
      configMapDependentResource, pvcDependentResource, jobDependentResource);
  }

}
