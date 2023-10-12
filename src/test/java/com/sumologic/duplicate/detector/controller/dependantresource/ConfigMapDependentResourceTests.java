package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanSpec;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

@ExtendWith(MockitoExtension.class)
public class ConfigMapDependentResourceTests extends BaseTests {

  @Mock
  private Context<DuplicateMessageScan> context;

  private ConfigMapDependentResource resource;

  @Test
  @DisplayName("Generates a config map for a single customer")
  void generateBasicConfigMap() {
    DuplicateMessageScan scan = createSingleCustomerScan();
    assertEqualsWithYaml(loadYaml(ConfigMap.class, getClass(), "/configmap/single-customer.yaml"),
      resource.desired(scan, context));
  }

  @Test
  @DisplayName("Generates a config map for multiple customers")
  void multipleCustomers() {
    DuplicateMessageScan scan = createMultipleCustomerScan();
    assertEqualsWithYaml(loadYaml(ConfigMap.class, getClass(), "/configmap/multiple-customers.yaml"),
      resource.desired(scan, context));
  }

  @Test
  @DisplayName("Generates a config map using the provided target object")
  void useProvidedTargetObject() {
    DuplicateMessageScanSpec spec = createSingleCustomerSpec();
    spec.targetObject = "blocks";
    DuplicateMessageScan scan = createScan(spec);
    assertEqualsWithYaml(loadYaml(ConfigMap.class, getClass(), "/configmap/blocks.yaml"),
      resource.desired(scan, context));
  }
  @Test
  @DisplayName("Generates a config map that splits the time range by the given period")
  void splitTimeRange() {
    DuplicateMessageScanSpec spec = new DuplicateMessageScanSpec(
      "2023-09-06T10:00:00-07:00", "2023-09-06T10:15:00-07:00", List.of("0000000000000005"));
    spec.timeRangeSegmentLength = "PT5m";
    assertEqualsWithYaml(loadYaml(ConfigMap.class, getClass(),"/configmap/single-customer-split-time-range.yaml"),
      resource.desired(createScan(spec), context));
  }

  @BeforeEach
  private void beforeEach() {
    resource = new ConfigMapDependentResource();
  }
}
