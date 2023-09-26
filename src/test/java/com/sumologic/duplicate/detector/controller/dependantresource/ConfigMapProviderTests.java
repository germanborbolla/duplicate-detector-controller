package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanSpec;
import io.fabric8.kubernetes.api.model.ConfigMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class ConfigMapProviderTests extends BaseTests {

  private ConfigMapProvider sut = new ConfigMapProvider();

  @Test
  @DisplayName("Generates a config map for a single customer")
  void generateBasicConfigMap() {
    DuplicateMessageScan scan = createSingleCustomerScan();
    assertEqualsWithYaml(loadYaml(ConfigMap.class, getClass(), "/configmap/single-customer.yaml"),
      sut.desired(scan, null));
  }

  @Test
  @DisplayName("Generates a config map for multiple customers")
  void multipleCustomers() {
    DuplicateMessageScan scan = createMultipleCustomerScan();
    assertEqualsWithYaml(loadYaml(ConfigMap.class, getClass(), "/configmap/multiple-customers.yaml"),
      sut.desired(scan, null));
  }

  @Test
  @DisplayName("Generates a config map using the provided target object")
  void useProvidedTargetObject() {
    DuplicateMessageScanSpec spec = createSingleCustomerSpec();
    spec.setTargetObject("blocks");
    DuplicateMessageScan scan = createScan(spec);
    assertEqualsWithYaml(loadYaml(ConfigMap.class, getClass(), "/configmap/blocks.yaml"),
      sut.desired(scan, null));
  }
}
