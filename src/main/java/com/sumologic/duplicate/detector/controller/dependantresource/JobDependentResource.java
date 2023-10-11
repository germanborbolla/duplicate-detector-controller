package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.Constants;
import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanStatus;
import com.sumologic.duplicate.detector.controller.customresource.Segment;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResourceConfig;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class JobDependentResource extends CRUDKubernetesDependentResource<Job, DuplicateMessageScan> implements BulkDependentResource<Job, DuplicateMessageScan> {

  public static JobDependentResource create(ReconcilerConfiguration.JobConfiguration configuration, KubernetesClient client) {
    JobDependentResource resource = new JobDependentResource(configuration);
    resource.setKubernetesClient(client);
    resource.configureWith(new KubernetesDependentResourceConfig<Job>()
      .setLabelSelector(Constants.RESOURCE_LABEL_SELECTOR));
    return resource;
  }

  private final ReconcilerConfiguration.JobConfiguration configuration;

  public JobDependentResource(ReconcilerConfiguration.JobConfiguration configuration) {
    super(Job.class);
    this.configuration = configuration;
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
    return nextSegments.collect(Collectors.toMap(s -> s.id, s -> createJobForSegment(scan, s,
      scan.getSpec().getSegments().size() == 1)));
  }

  @Override
  public Map<String, Job> getSecondaryResources(DuplicateMessageScan scan, Context<DuplicateMessageScan> context) {
    return context.getSecondaryResourcesAsStream(Job.class)
      .filter(job -> job.getMetadata() != null && job.getMetadata().getLabels() != null
        && job.getMetadata().getLabels().containsKey(Constants.JOB_SEGMENT_LABEL_KEY))
      .collect(Collectors.toMap(job -> job.getMetadata().getLabels().get(Constants.JOB_SEGMENT_LABEL_KEY),
        Function.identity()));
  }

  private Job createJobForSegment(DuplicateMessageScan scan, Segment segment, boolean usePVC) {
    String baseJobYaml = "/baseDependantResources/job.yaml";
    if (configuration.isUseIntegrationTestJob()) {
      baseJobYaml = "/baseDependantResources/integration-test-job.yaml";
    }
    Job base = loadYaml(Job.class, getClass(), baseJobYaml);
    PodSpecBuilder podSpecBuilder = new PodSpecBuilder(base.getSpec().getTemplate().getSpec());
    configureContainer(podSpecBuilder, segment);
    configureVolumes(podSpecBuilder, scan.getMetadata().getName(), usePVC);
    return new JobBuilder(base)
      .withMetadata(new ObjectMetaBuilder(scan.buildDependentObjectMetadata())
        .withName(String.format("%s-%s", scan.getMetadata().getName(), segment.id))
        .addToLabels(Constants.JOB_SEGMENT_LABEL_KEY, segment.id)
        .build())
      .editSpec()
      .editTemplate().withSpec(podSpecBuilder.build()).endTemplate()
      .withBackoffLimit(scan.getSpec().retriesPerSegment)
      .endSpec()
      .build();
  }
  private void configureContainer(PodSpecBuilder podSpecBuilder, Segment segment) {
    PodSpecFluent<PodSpecBuilder>.ContainersNested<PodSpecBuilder> containerBuilder = podSpecBuilder.editFirstContainer();
    if (configuration.isUseIntegrationTestJob()) {
      containerBuilder
        .withEnv(new EnvVarBuilder().withName("SLEEP_TIME")
          .withValue(String.valueOf(configuration.getSleepTimeForIntegrationTest())).build());
    } else {
      containerBuilder
        .withImage(configuration.getSystemToolsImage());
    }
    containerBuilder.addNewEnv().withName(Constants.JOB_COMPLETION_INDEX_ENV_NAME).withValue(segment.id).endEnv();
    containerBuilder.endContainer();
  }

  private void configureVolumes(PodSpecBuilder podSpecBuilder, String name, boolean usePVC) {
    if (configuration.isUseIntegrationTestJob()) {
      podSpecBuilder.addNewVolume().withName("config").withNewConfigMap()
        .withDefaultMode(420).withName(name)
        .endConfigMap().endVolume();
    } else {
      podSpecBuilder.addNewVolume().withName("config").withNewProjected()
        .withDefaultMode(420)
        .addNewSource().withNewConfigMap().withName("duplicate-detector-properties").endConfigMap().endSource()
        .addNewSource().withNewConfigMap().withName(name).endConfigMap().endSource()
        .endProjected().endVolume();
    }
    if (usePVC) {
      podSpecBuilder.addNewVolume().withName("duplicate-detector-pvc")
        .withNewPersistentVolumeClaim().withClaimName(name).endPersistentVolumeClaim()
        .endVolume();
    } else {
      podSpecBuilder.addNewVolume().withName("duplicate-detector-pvc")
        .withNewEmptyDir().endEmptyDir().endVolume();
    }
  }
}
