package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanStatus;
import com.sumologic.duplicate.detector.controller.customresource.Segment;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@ControllerConfiguration
public class DuplicateMessageScanReconciler implements Reconciler<DuplicateMessageScan>, ErrorStatusHandler<DuplicateMessageScan>, EventSourceInitializer<DuplicateMessageScan> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final KubernetesDependentResource<ConfigMap, DuplicateMessageScan> configMapDependentResource;
  private final KubernetesDependentResource<PersistentVolumeClaim, DuplicateMessageScan> pvcDependentResource;
  private final KubernetesDependentResource<Job, DuplicateMessageScan> jobDependentResource;
  private final Workflow<DuplicateMessageScan> nonParallelWorkflow;
  private final Workflow<DuplicateMessageScan> parallelWorkflow;

  public DuplicateMessageScanReconciler(KubernetesClient client, ReconcilerConfiguration reconcilerConfiguration) {
    configMapDependentResource = ProviderKubernetesDependentResource.create(ConfigMap.class,
      new ConfigMapProvider(), client);
    pvcDependentResource = ProviderKubernetesDependentResource.create(PersistentVolumeClaim.class,
      new PersistentVolumeClaimProvider(reconcilerConfiguration.persistentVolumeConfiguration), client);
    jobDependentResource = ProviderKubernetesDependentResource.create(Job.class,
      new JobProvider(reconcilerConfiguration.jobConfiguration), client);

    nonParallelWorkflow = new WorkflowBuilder<DuplicateMessageScan>()
      .addDependentResource(configMapDependentResource)
      .addDependentResource(pvcDependentResource)
      .addDependentResource(jobDependentResource).dependsOn(configMapDependentResource, pvcDependentResource)
      .build();

    parallelWorkflow = new WorkflowBuilder<DuplicateMessageScan>()
      .addDependentResource(configMapDependentResource)
      .addDependentResource(jobDependentResource).dependsOn(configMapDependentResource)
      .build();
  }

  @Override
  public UpdateControl<DuplicateMessageScan> reconcile(DuplicateMessageScan scan,
                                                       Context<DuplicateMessageScan> context) {
    logger.info("Reconciling scan {}", scan);
    if (scan.getSpec().getSegments().size() == 1) {
      return singleReconcile(scan, context, true);
    } else {
      return multipleReconcile(scan, context, true);
    }
  }

  @Override
  public ErrorStatusUpdateControl<DuplicateMessageScan> updateErrorStatus(DuplicateMessageScan resource,
                                                                          Context<DuplicateMessageScan> context,
                                                                          Exception e) {
    DuplicateMessageScanStatus status = resource.getOrCreateStatus();
    status.failed(e.getMessage());
    return ErrorStatusUpdateControl.patchStatus(resource.withStatus(status));
  }

  @Override
  public Map<String, EventSource> prepareEventSources(EventSourceContext<DuplicateMessageScan> context) {
    return EventSourceInitializer.nameEventSourcesFromDependentResource(context,
      configMapDependentResource, pvcDependentResource, jobDependentResource);
  }

  UpdateControl<DuplicateMessageScan> singleReconcile(DuplicateMessageScan scan,
                                                      Context<DuplicateMessageScan> context,
                                                      boolean invokeWorkflow) {
    if (invokeWorkflow) {
      nonParallelWorkflow.reconcile(scan, context);
    }
    DuplicateMessageScanStatus status = scan.getOrCreateStatus();
    context.getSecondaryResource(Job.class).ifPresent(job -> {
      if (job.getStatus() != null && job.getStatus().getSucceeded() != null && job.getStatus().getSucceeded() == 1) {
        status.completed();
      } else if (job.getStatus() != null && job.getStatus().getFailed() != null && job.getStatus().getFailed() == 1) {
        status.failed("All attempts to execute scan failed");
      } else {
        status.segments.forEach(Segment::processing);
        status.updateSegmentsCounts();
      }
    });
    return UpdateControl.patchStatus(scan.withStatus(status));
  }

  UpdateControl<DuplicateMessageScan> multipleReconcile(DuplicateMessageScan scan,
                                                        Context<DuplicateMessageScan> context,
                                                        boolean invokeWorkflow) {
    if (invokeWorkflow) {
      parallelWorkflow.reconcile(scan, context);
    }
    return UpdateControl.patchStatus(scan.withStatus(scan.getOrCreateStatus()));

  }
}
