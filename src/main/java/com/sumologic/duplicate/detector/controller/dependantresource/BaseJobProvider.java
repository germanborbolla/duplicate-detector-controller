package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;

import java.util.List;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class BaseJobProvider {
  protected ReconcilerConfiguration.JobConfiguration configuration;

  public BaseJobProvider(ReconcilerConfiguration.JobConfiguration configuration) {
    this.configuration = configuration;
  }

  protected Job desiredJob(String name, List<String> arguments, ObjectMeta dependentObjectMetadata) {
    String baseJobYaml = "/baseDependantResources/job.yaml";
    if (configuration.isUseIntegrationTestJob()) {
      baseJobYaml = "/baseDependantResources/integration-test-job.yaml";
    }
    Job base = loadYaml(Job.class, getClass(), baseJobYaml);
    PodSpecBuilder podSpecBuilder = new PodSpecBuilder(base.getSpec().getTemplate().getSpec());
    configureContainer(arguments, podSpecBuilder);
    configureVolumes(name, podSpecBuilder);
    return new JobBuilder(base)
      .withMetadata(dependentObjectMetadata)
      .editSpec().editTemplate()
      .withSpec(podSpecBuilder.build())
      .endTemplate().endSpec().build();
  }

  private void configureContainer(List<String> arguments, PodSpecBuilder podSpecBuilder) {
    if (configuration.isUseIntegrationTestJob()) {
      podSpecBuilder.editFirstContainer()
        .withEnv(new EnvVarBuilder().withName("SLEEP_TIME")
          .withValue(String.valueOf(configuration.getSleepTimeForIntegrationTest())).build())
        .endContainer();
    } else {
      podSpecBuilder
        .editFirstContainer()
        .withImage(configuration.getSystemToolsImage())
        .addAllToArgs(arguments)
        .endContainer();
    }
  }

  private void configureVolumes(String name, PodSpecBuilder podSpecBuilder) {
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
    podSpecBuilder.addNewVolume().withName("duplicate-detector-pvc")
      .withNewPersistentVolumeClaim().withClaimName(name).endPersistentVolumeClaim()
      .endVolume();
  }
}
