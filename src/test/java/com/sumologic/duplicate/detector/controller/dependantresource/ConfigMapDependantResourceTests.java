package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScanSpec;
import io.fabric8.kubernetes.api.model.ConfigMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class ConfigMapDependantResourceTests extends BaseTests {

  @DisplayName("Generates expected config map that scans indices")
  @Test
  void generateBasicConfigMap() {
    ConfigMapDependantResource sut = new ConfigMapDependantResource();
    SingleDuplicateMessageScan scan = createScan();
    assertEqualsWithYaml(loadYaml(ConfigMap.class, getClass(), "/configmap/basic.yaml"),
      sut.desired(scan, null));
  }

  @DisplayName("Generated a config map that scans the target object")
  @Test
  void configMapWithDesiredTargetObject() {
    ConfigMapDependantResource sut = new ConfigMapDependantResource();
    SingleDuplicateMessageScan scan = createScan(new SingleDuplicateMessageScanSpec().setTargetObject("blocks"));
    assertEqualsWithYaml(loadYaml(ConfigMap.class, getClass(), "/configmap/blocks.yaml"),
      sut.desired(scan, null));
  }
}
