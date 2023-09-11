package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.List;

public class JobProvider extends BaseJobProvider implements DesiredProvider<Job, SingleDuplicateMessageScan> {

    public JobProvider(ReconcilerConfiguration.JobConfiguration configuration) {
        super(configuration);
    }

    @Override
    public Job desired(SingleDuplicateMessageScan scan, Context<SingleDuplicateMessageScan> context) {
        return desiredJob(scan.getMetadata().getName(),
          List.of("--start-time " + scan.getSpec().getStartTime(),
            "--end-time " + scan.getSpec().getEndTime(),
            "--customers " + scan.getSpec().getCustomer()),
          scan.buildDependentObjectMetadata());
    }

}
