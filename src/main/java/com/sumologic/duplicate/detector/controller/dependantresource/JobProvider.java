package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.List;
import java.util.function.Function;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class JobProvider<R extends HasMetadata> implements DesiredProvider<Job, R> {

    public static JobProvider<SingleDuplicateMessageScan> createForSingleDuplicateMessageScan(ReconcilerConfiguration.JobConfiguration configuration) {
        return new JobProvider<>(configuration,
          SingleDuplicateMessageScan::buildDependentObjectMetadata);
    }

    private final ReconcilerConfiguration.JobConfiguration configuration;
    private final Function<R, ObjectMeta> metadataFunction;

    public JobProvider(ReconcilerConfiguration.JobConfiguration configuration,
                       Function<R, ObjectMeta> metadataFunction) {
        this.configuration = configuration;
        this.metadataFunction = metadataFunction;
    }

    @Override
    public Job desired(R scan, Context<R> context) {
        String baseJobYaml = "/baseDependantResources/job.yaml";
        if (configuration.isUseIntegrationTestJob()) {
            baseJobYaml = "/baseDependantResources/integration-test-job.yaml";
        }
        Job base = loadYaml(Job.class, getClass(), baseJobYaml);
        PodSpecBuilder podSpecBuilder = new PodSpecBuilder(base.getSpec().getTemplate().getSpec());
        configureContainer(podSpecBuilder);
        configureVolumes(scan.getMetadata().getName(), podSpecBuilder);
        return new JobBuilder(base)
          .withMetadata(metadataFunction.apply(scan))
          .editSpec().editTemplate()
          .withSpec(podSpecBuilder.build())
          .endTemplate().endSpec().build();
    }

    private void configureContainer(PodSpecBuilder podSpecBuilder) {
        if (configuration.isUseIntegrationTestJob()) {
            podSpecBuilder.editFirstContainer()
              .withEnv(new EnvVarBuilder().withName("SLEEP_TIME")
                .withValue(String.valueOf(configuration.getSleepTimeForIntegrationTest())).build())
              .endContainer();
        } else {
            podSpecBuilder
              .editFirstContainer()
              .withImage(configuration.getSystemToolsImage())
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
