package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobFluent;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.Map;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class JobProvider implements DesiredProvider<Job, DuplicateMessageScan> {
    private final ReconcilerConfiguration.JobConfiguration configuration;

    public JobProvider(ReconcilerConfiguration.JobConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Job desired(DuplicateMessageScan scan, Context<DuplicateMessageScan> context) {
        String baseJobYaml = "/baseDependantResources/job.yaml";
        if (configuration.isUseIntegrationTestJob()) {
            baseJobYaml = "/baseDependantResources/integration-test-job.yaml";
        }
        Job base = loadYaml(Job.class, getClass(), baseJobYaml);
        PodSpecBuilder podSpecBuilder = new PodSpecBuilder(base.getSpec().getTemplate().getSpec());
        configureContainer(podSpecBuilder);
        configureVolumes(scan, podSpecBuilder);
        JobFluent<JobBuilder>.SpecNested<JobBuilder> jobSpecBuilder = new JobBuilder(base)
          .withMetadata(scan.buildDependentObjectMetadata())
          .editSpec();
        jobSpecBuilder.editTemplate().withSpec(podSpecBuilder.build()).endTemplate();
        Map<String, Map<String, String>> inputs = scan.getSpec().buildInputs();
        if (inputs.size() > 1) {
            jobSpecBuilder
              .withCompletions(inputs.size())
              .withCompletionMode("Indexed")
              .withParallelism(scan.getSpec().getMaxParallelScans());
        }
        jobSpecBuilder.withBackoffLimit(scan.getSpec().getRetriesPerSegment() * inputs.size());
        return jobSpecBuilder.endSpec().build();
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

    private void configureVolumes(DuplicateMessageScan scan, PodSpecBuilder podSpecBuilder) {
        String name = scan.getMetadata().getName();
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
        if (scan.getSpec().getMaxParallelScans() == 1) {
            podSpecBuilder.addNewVolume().withName("duplicate-detector-pvc")
              .withNewPersistentVolumeClaim().withClaimName(name).endPersistentVolumeClaim()
              .endVolume();
        } else {
            podSpecBuilder.addNewVolume().withName("duplicate-detector-pvc")
              .withNewEmptyDir().endEmptyDir().endVolume();
        }
    }
}
