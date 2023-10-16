package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanStatus;
import com.sumologic.duplicate.detector.controller.customresource.Segment;
import com.sumologic.duplicate.detector.controller.dependantresource.ConfigMapDependentResource;
import com.sumologic.duplicate.detector.controller.dependantresource.JobDependentResource;
import com.sumologic.duplicate.detector.controller.dependantresource.PersistentVolumeClaimDependentResource;
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

import java.time.Clock;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

@ControllerConfiguration
public class DuplicateMessageScanReconciler implements Reconciler<DuplicateMessageScan>, ErrorStatusHandler<DuplicateMessageScan>, EventSourceInitializer<DuplicateMessageScan> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final KubernetesDependentResource<ConfigMap, DuplicateMessageScan> configMapDependentResource;
  private final KubernetesDependentResource<PersistentVolumeClaim, DuplicateMessageScan> pvcDependentResource;
  private final KubernetesDependentResource<Job, DuplicateMessageScan> jobDependentResource;
  private final Workflow<DuplicateMessageScan> workflow;
  private final Clock clock;

  public DuplicateMessageScanReconciler(KubernetesClient client, ReconcilerConfiguration reconcilerConfiguration) {
    this(client, reconcilerConfiguration, Clock.systemUTC());
  }

  public DuplicateMessageScanReconciler(KubernetesClient client, ReconcilerConfiguration reconcilerConfiguration,
                                        Clock clock) {
    configMapDependentResource = ConfigMapDependentResource.create(client);
    pvcDependentResource = PersistentVolumeClaimDependentResource
      .create(reconcilerConfiguration.persistentVolumeConfiguration, client);
    jobDependentResource = JobDependentResource.create(reconcilerConfiguration.jobConfiguration, client);

    workflow = new WorkflowBuilder<DuplicateMessageScan>()
      .addDependentResource(configMapDependentResource)
      .addDependentResource(pvcDependentResource)
      .addDependentResource(jobDependentResource).dependsOn(configMapDependentResource, pvcDependentResource)
      .build();

    this.clock = clock;
  }

  @Override
  public UpdateControl<DuplicateMessageScan> reconcile(DuplicateMessageScan scan,
                                                       Context<DuplicateMessageScan> context) {
    DuplicateMessageScanStatus updatedStatus = calculateStatus(scan, context);
    workflow.reconcile(scan, context);
    UpdateControl<DuplicateMessageScan> control = UpdateControl.patchStatus(scan.withStatus(updatedStatus));
    logger.debug("Reconciled scan {} with control {}", scan, control);
    return control;
  }

  @Override
  public ErrorStatusUpdateControl<DuplicateMessageScan> updateErrorStatus(DuplicateMessageScan resource,
                                                                          Context<DuplicateMessageScan> context,
                                                                          Exception e) {
    DuplicateMessageScanStatus status = getOrCreateStatus(resource);
    status.failed(e.getMessage());
    return ErrorStatusUpdateControl.patchStatus(resource.withStatus(status));
  }

  @Override
  public Map<String, EventSource> prepareEventSources(EventSourceContext<DuplicateMessageScan> context) {
    return EventSourceInitializer.nameEventSourcesFromDependentResource(context,
      configMapDependentResource, pvcDependentResource, jobDependentResource);
  }

  DuplicateMessageScanStatus calculateStatus(DuplicateMessageScan scan,
                                             Context<DuplicateMessageScan> context) {
    DuplicateMessageScanStatus status = getOrCreateStatus(scan);
    status.lastUpdateTime = currentDate();
    context.getSecondaryResources(Job.class).forEach(job -> Optional.ofNullable(job.getMetadata().getLabels())
      .flatMap(labels -> Optional.ofNullable(labels.get(Constants.SEGMENT_LABEL_KEY))
        .map(Integer::parseInt)).ifPresent(index -> {
          Segment segment = status.segments.get(index);
        if (job.getStatus() != null && job.getStatus().getSucceeded() != null
          && job.getStatus().getSucceeded() == 1) {
            segment.completed();
          } else if (job.getStatus() != null && job.getStatus().getFailed() != null
          && job.getStatus().getFailed() == 1) {
            segment.failed();
          } else {
            segment.processing();
          }
      })
    );
    if (status.segments.stream().allMatch(Segment::isFinished)) {
      status.completionTime = currentDate();
      if (status.segments.stream().anyMatch(s -> s.status == Segment.SegmentStatus.FAILED)) {
        status.failed("One or more jobs failed");
      } else {
        status.completed();
      }
    }
    status.updateSegmentsCounts();
    return status;
  }
  public DuplicateMessageScanStatus getOrCreateStatus(DuplicateMessageScan scan) {
    DuplicateMessageScanStatus status = scan.getStatus();
    if (status == null) {
      status = new DuplicateMessageScanStatus(scan.getSpec().getSegments(), currentDate());
      scan.setStatus(status);
    }
    return status;
  }
  private Date currentDate() {
    return new Date(clock.millis());
  }
}
