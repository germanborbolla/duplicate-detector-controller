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
import io.javaoperatorsdk.operator.api.reconciler.*;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Workflow;
import io.javaoperatorsdk.operator.processing.dependent.workflow.WorkflowBuilder;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;

import java.util.Map;

@ControllerConfiguration
public class SingleDuplicateMessageScanReconciler implements Reconciler<SingleDuplicateMessageScan>, ErrorStatusHandler<SingleDuplicateMessageScan>, EventSourceInitializer<SingleDuplicateMessageScan> {

  private final KubernetesDependentResource<ConfigMap, SingleDuplicateMessageScan> configMapDependentResource;
  private final KubernetesDependentResource<PersistentVolumeClaim, SingleDuplicateMessageScan> pvcDependentResource;
  private final KubernetesDependentResource<Job, SingleDuplicateMessageScan> jobDependentResource;
  private final Workflow<SingleDuplicateMessageScan> workflow;

  public SingleDuplicateMessageScanReconciler(KubernetesClient client, ReconcilerConfiguration reconcilerConfiguration) {
    configMapDependentResource = ProviderKubernetesDependentResource.create(ConfigMap.class,
      new ConfigMapProvider(), client);
    pvcDependentResource = ProviderKubernetesDependentResource.create(PersistentVolumeClaim.class,
      new PersistentVolumeClaimProvider(reconcilerConfiguration.persistentVolumeConfiguration), client);
    jobDependentResource = ProviderKubernetesDependentResource.create(Job.class,
      new JobProvider(reconcilerConfiguration.jobConfiguration), client);

    // TODO(panda, 9/8/23): job shouldn't be created if PVC cannot be provisioned 
    workflow = new WorkflowBuilder<SingleDuplicateMessageScan>()
      .addDependentResource(configMapDependentResource)
      .addDependentResource(pvcDependentResource)
      .addDependentResource(jobDependentResource).dependsOn(configMapDependentResource, pvcDependentResource)
      .build();
  }

  @Override
  public UpdateControl<SingleDuplicateMessageScan> reconcile(SingleDuplicateMessageScan scan,
                                                             Context<SingleDuplicateMessageScan> context) throws Exception {
    // TODO(panda, 9/6/23): don't reconcile the same request if the job has already completed for it
    workflow.reconcile(scan, context);

    return UpdateControl.patchStatus(scan.withStatus(
      new DuplicateMessageScanStatus(context.getSecondaryResource(Job.class).orElseThrow().getStatus())));
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
