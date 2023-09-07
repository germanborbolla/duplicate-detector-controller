package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScanSpec;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class PersistentVolumeClaimProviderTests extends BaseTests {

  @DisplayName("Creates a PVC with the default size")
  @Test
  void testCreateAVolumeWithDefaultSize() {
    SingleDuplicateMessageScan scan = createScan();
    PersistentVolumeClaimProvider sut = new PersistentVolumeClaimProvider();
    assertEqualsWithYaml(loadYaml(PersistentVolumeClaim.class, getClass(), "/pvc/basic.yaml"),
      sut.desired(scan, null));
  }

  @DisplayName("Creates a PVC with the provided size")
  @Test
  void testCustomSize() {
    SingleDuplicateMessageScanSpec spec = new SingleDuplicateMessageScanSpec();
    spec.setVolumeSize("100Gi");
    SingleDuplicateMessageScan scan = createScan(spec);
    PersistentVolumeClaimProvider sut = new PersistentVolumeClaimProvider();
    assertEqualsWithYaml(loadYaml(PersistentVolumeClaim.class, getClass(), "/pvc/custom-size.yaml"),
      sut.desired(scan, null));
  }
}
