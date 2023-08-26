package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScanSpec;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class PersistentVolumeClaimDependantResourceTests extends BaseTests {

  @DisplayName("Creates a PVC with the default size")
  @Test
  void testCreateAVolumeWithDefaultSize() {
    SingleDuplicateMessageScan scan = createScan();
    PersistentVolumeClaimDependantResource sut = new PersistentVolumeClaimDependantResource();
    assertEqualsWithYaml(loadYaml(PersistentVolumeClaim.class, getClass(), "/pvc/basic.yaml"),
      sut.desired(scan, null));
  }

  @DisplayName("Creates a PVC with the provided size")
  @Test
  void testCustomSize() {
    SingleDuplicateMessageScan scan = createScan(new SingleDuplicateMessageScanSpec().setVolumeSize("100Gi"));
    PersistentVolumeClaimDependantResource sut = new PersistentVolumeClaimDependantResource();
    assertEqualsWithYaml(loadYaml(PersistentVolumeClaim.class, getClass(), "/pvc/custom-size.yaml"),
      sut.desired(scan, null));
  }
}
