package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class JobProvider implements DesiredProvider<Job, SingleDuplicateMessageScan> {

    private ReconcilerConfiguration.JobConfiguration configuration;

    public JobProvider(ReconcilerConfiguration.JobConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Job desired(SingleDuplicateMessageScan scan, Context<SingleDuplicateMessageScan> context) {
        String baseJobYaml = "/baseDependantResources/job.yaml";
        if (configuration.isUseIntegrationTestJob()) {
            baseJobYaml = "/baseDependantResources/integration-test-job.yaml";
        }
        Job base = loadYaml(Job.class, getClass(), baseJobYaml);
        PodSpecBuilder podSpecBuilder = new PodSpecBuilder(base.getSpec().getTemplate().getSpec());
        configureContainer(scan, podSpecBuilder);
        configureVolumes(scan, podSpecBuilder);
        return new JobBuilder(base)
          .withMetadata(scan.buildDependentObjectMetadata())
          .editSpec().editTemplate()
          .withSpec(podSpecBuilder.build())
          .endTemplate().endSpec().build();
    }

    private void configureContainer(SingleDuplicateMessageScan scan, PodSpecBuilder podSpecBuilder) {
        if (configuration.isUseIntegrationTestJob()) {
            podSpecBuilder.editFirstContainer()
              .withEnv(new EnvVarBuilder().withName("SLEEP_TIME")
                .withValue(String.valueOf(configuration.getSleepTimeForIntegrationTest())).build())
              .endContainer();
        } else {
            podSpecBuilder
              .editFirstContainer()
              .withImage(configuration.getSystemToolsImage())
              .addToArgs("--start-time " + scan.getSpec().getStartTime(),
                "--end-time " + scan.getSpec().getEndTime(),
                "--customers " + scan.getSpec().getCustomer())
              .endContainer();
        }
    }

    private void configureVolumes(SingleDuplicateMessageScan scan, PodSpecBuilder podSpecBuilder) {
        if (configuration.isUseIntegrationTestJob()) {
            podSpecBuilder.addNewVolume().withName("config").withNewConfigMap()
              .withDefaultMode(420).withName(scan.getMetadata().getName())
              .endConfigMap().endVolume();
        } else {
            podSpecBuilder.addNewVolume().withName("config").withNewProjected()
              .withDefaultMode(420)
              .addNewSource().withNewConfigMap().withName("duplicate-detector-properties").endConfigMap().endSource()
              .addNewSource().withNewConfigMap().withName(scan.getMetadata().getName()).endConfigMap().endSource()
              .endProjected().endVolume();
        }
        podSpecBuilder.addNewVolume().withName("duplicate-detector-pvc")
          .withNewPersistentVolumeClaim().withClaimName(scan.getMetadata().getName()).endPersistentVolumeClaim()
          .endVolume();
    }
}
