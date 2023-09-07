package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScanSpec;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class JobProviderTests extends BaseTests {

  @DisplayName("Creates a job with the arguments from the spec")
  @Test
  void testCreateBasicJob() {
    SingleDuplicateMessageScan scan = createScan(new SingleDuplicateMessageScanSpec("startTime", "endTime",
      "0000000000000005"));
    JobProvider sut = new JobProvider("system-tools:123");
    assertEqualsWithYaml(loadYaml(Job.class, getClass(), "/job/basic.yaml"), sut.desired(scan, null));

  }
}
