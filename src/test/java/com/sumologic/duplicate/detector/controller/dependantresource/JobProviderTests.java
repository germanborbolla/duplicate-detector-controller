package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanSpec;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class JobProviderTests extends BaseTests {

  private JobProvider sut = new JobProvider(new ReconcilerConfiguration.JobConfiguration(
    "system-tools:123", false, 5));
  @Test
  @DisplayName("Generates a job for a scan with a single customer")
  void singleCustomer() {
    DuplicateMessageScan scan = createSingleCustomerScan();
    assertEqualsWithYaml(loadYaml(Job.class, getClass(), "/job/single-customer.yaml"),
      sut.desired(scan, null));
  }

  @Test
  @DisplayName("Generates a job that executes scans in parallel")
  void multipleCustomers() {
    DuplicateMessageScan scan = createMultipleCustomerScan();
    assertEqualsWithYaml(loadYaml(Job.class, getClass(), "/job/multiple-customers.yaml"),
      sut.desired(scan, null));
  }

  @Test
  @DisplayName("Use the parallelism from the spec")
  void useParallelismFromSpec() {
    DuplicateMessageScanSpec spec = createMultipleCustomerSpec();
    spec.setMaxParallelScans(3);
    DuplicateMessageScan scan = createScan(spec);
    assertEqualsWithYaml(loadYaml(Job.class, getClass(), "/job/parallelism-3.yaml"),
      sut.desired(scan, null));
  }
}
