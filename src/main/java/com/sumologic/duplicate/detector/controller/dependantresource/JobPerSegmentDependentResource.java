package com.sumologic.duplicate.detector.controller.dependantresource;

import com.google.common.collect.Maps;
import com.sumologic.duplicate.detector.controller.Constants;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanStatus;
import com.sumologic.duplicate.detector.controller.customresource.Segment;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResourceConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JobPerSegmentDependentResource extends CRUDKubernetesDependentResource<Job, DuplicateMessageScan> implements BulkDependentResource<Job, DuplicateMessageScan> {

  public static JobPerSegmentDependentResource create(JobProvider jobProvider, KubernetesClient client) {
    JobPerSegmentDependentResource resource = new JobPerSegmentDependentResource(jobProvider);
    resource.setKubernetesClient(client);
    resource.configureWith(new KubernetesDependentResourceConfig<Job>()
      .setLabelSelector(Constants.RESOURCE_LABEL_SELECTOR));
    return resource;
  }

  private final JobProvider jobProvider;
  public JobPerSegmentDependentResource(JobProvider jobProvider) {
    super(Job.class);
    this.jobProvider = jobProvider;
  }

  @Override
  public Map<String, Job> desiredResources(DuplicateMessageScan scan, Context<DuplicateMessageScan> context) {
    Stream<Segment> nextSegments;
    DuplicateMessageScanStatus status = scan.getStatus();
    int parallelism = scan.getSpec().maxParallelScans;
    if (status == null) {
      nextSegments = scan.getSpec().getSegments().stream().limit(parallelism);
    } else {
      nextSegments = status.segments.stream().filter(Segment::isPendingOrProcessing).limit(parallelism);
    }
    return nextSegments.collect(Collectors.toMap(s -> s.id, s -> createJobForSegment(scan, s)));
  }

  @Override
  public Map<String, Job> getSecondaryResources(DuplicateMessageScan scan, Context<DuplicateMessageScan> context) {
    return context.getSecondaryResourcesAsStream(Job.class)
      .filter(job -> job.getMetadata() != null && job.getMetadata().getLabels() != null
        && job.getMetadata().getLabels().containsKey(Constants.JOB_SEGMENT_LABEL_KEY))
      .collect(Collectors.toMap(job -> job.getMetadata().getLabels().get(Constants.JOB_SEGMENT_LABEL_KEY),
        Function.identity()));
  }

  private Job createJobForSegment(DuplicateMessageScan scan, Segment segment) {
    ObjectMeta objectMeta = new ObjectMetaBuilder(scan.buildDependentObjectMetadata())
      .withName(String.format("%s-%s", scan.getMetadata().getName(), segment.id))
      .addToLabels(Constants.JOB_SEGMENT_LABEL_KEY, segment.id)
      .build();
    return jobProvider.jobBuilder(scan.getMetadata().getName(), objectMeta, 1, 1,
        scan.getSpec().retriesPerSegment, false)
      .editSpec().editTemplate().editSpec().editFirstContainer()
      .addNewEnv().withName(Constants.JOB_COMPLETION_INDEX_ENV_NAME).withValue(segment.id).endEnv()
      .endContainer().endSpec().endTemplate().endSpec()
      .build();
  }
}
