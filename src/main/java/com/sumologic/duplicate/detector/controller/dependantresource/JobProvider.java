package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.Constants;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.Objects;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class JobProvider implements DesiredProvider<Job, SingleDuplicateMessageScan> {

    private String systemToolsImage = "";

    public JobProvider() {
        this(Objects.requireNonNull(System.getenv(Constants.SYSTEM_TOOLS_IMAGE_ENV_NAME)));
    }

    public JobProvider(String systemToolsImage) {
        this.systemToolsImage = systemToolsImage;
    }

    @Override
    public Job desired(SingleDuplicateMessageScan scan, Context<SingleDuplicateMessageScan> context) {
        Job base = loadYaml(Job.class, getClass(), "/baseDependantResources/job.yaml");
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
        podSpecBuilder
          .editFirstContainer()
          .withImage(systemToolsImage)
          .addToArgs("--start-time " + scan.getSpec().getStartTime(),
            "--end-time " + scan.getSpec().getEndTime(),
            "--customers " + scan.getSpec().getCustomer())
          .endContainer();
    }

    private void configureVolumes(SingleDuplicateMessageScan scan, PodSpecBuilder podSpecBuilder) {
        podSpecBuilder.addNewVolume().withName("config").withNewProjected()
          .withDefaultMode(420)
          .addNewSource().withNewConfigMap().withName("duplicate-detector-properties").endConfigMap().endSource()
          .addNewSource().withNewConfigMap().withName(scan.getMetadata().getName()).endConfigMap().endSource()
          .endProjected().endVolume()
          .addNewVolume().withName("duplicate-detector-pvc")
          .withNewPersistentVolumeClaim().withClaimName(scan.getMetadata().getName()).endPersistentVolumeClaim()
          .endVolume();
    }
}
