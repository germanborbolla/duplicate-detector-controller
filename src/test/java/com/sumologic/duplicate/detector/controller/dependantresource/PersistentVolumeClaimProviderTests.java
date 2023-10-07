package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanSpec;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;

public class PersistentVolumeClaimProviderTests extends BaseTests {

  private ReconcilerConfiguration.PersistentVolumeConfiguration defaultConfiguration =
    new ReconcilerConfiguration.PersistentVolumeConfiguration("gp2", "300Gi");
  private PersistentVolumeClaimProvider sut = new PersistentVolumeClaimProvider(defaultConfiguration);

  @DisplayName("Creates a PVC with the default size")
  @Test
  void testCreateAVolumeWithDefaultSize() {
    DuplicateMessageScan scan = createMultipleCustomerScan();
    assertEqualsWithYaml(loadYaml(PersistentVolumeClaim.class, getClass(), "/pvc/basic.yaml"),
      sut.desired(scan, null));
  }

  @DisplayName("Creates a PVC with the provided size")
  @Test
  void testCustomSize() {
    DuplicateMessageScanSpec spec = createMultipleCustomerSpec();
    spec.volumeSize = "100Gi";
    DuplicateMessageScan scan = createScan(spec);
    assertEqualsWithYaml(loadYaml(PersistentVolumeClaim.class, getClass(), "/pvc/custom-size.yaml"),
      sut.desired(scan, null));
  }
}
