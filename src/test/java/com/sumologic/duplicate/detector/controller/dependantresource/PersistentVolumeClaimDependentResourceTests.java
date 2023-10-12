package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.Constants;
import com.sumologic.duplicate.detector.controller.ReconcilerConfiguration;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanSpec;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanStatus;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.javaoperatorsdk.operator.ReconcilerUtils.loadYaml;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class PersistentVolumeClaimDependentResourceTests extends BaseTests {

  @Mock
  private Context<DuplicateMessageScan> context;

  private ReconcilerConfiguration.PersistentVolumeConfiguration configuration =
    new ReconcilerConfiguration.PersistentVolumeConfiguration("gp2", "300Gi");
  private PersistentVolumeClaimDependentResource resource;

  private DuplicateMessageScanSpec spec = new DuplicateMessageScanSpec("2023-09-06T10:00:00-07:00",
    "2023-09-06T10:15:00-07:00", List.of("0000000000000005", "0000000000000006"));
  private DuplicateMessageScan scan;

  @Nested
  class GetSecondaryResources {
    @Test
    void returnEmptyWhenNoClaimsExist() {
      initClaims(Stream.empty());

      assertEquals(Map.of(), resource.getSecondaryResources(scan, context));
    }

    @Test
    void ignoreClaimsWithNoLabels() {
      initClaims(Stream.of(
        new PersistentVolumeClaimBuilder().build(),
        new PersistentVolumeClaimBuilder().withMetadata(new ObjectMeta()).build(),
        new PersistentVolumeClaimBuilder().withMetadata(
          new ObjectMetaBuilder().withLabels(Map.of("somelabel", "value")).build()).build()));

      assertEquals(Map.of(), resource.getSecondaryResources(scan, context));
    }

    @Test
    void mapClaimsByTheirLabel() {
      PersistentVolumeClaim claim0 = claimForSegment(0);
      PersistentVolumeClaim claim1 = claimForSegment(1);
      initClaims(Stream.of(claim0, claim1));

      assertEquals(Map.of("0", claim0, "1", claim1), resource.getSecondaryResources(scan, context));
    }
  }

  @Nested
  class DesiredResources {
    @Test
    void returnAPVCForEachSegment() {
      verifyClaims(Map.of("0", claimForSegment(0), "1", claimForSegment(1)),
        resource.desiredResources(scan, context));
    }

    @Test
    void useTheSizeFromTheSpec() {
      spec.volumeSize = "100Gi";
      verifyClaims(Map.of("0", claimForSegment(0, "/pvc/custom-size.yaml"),
          "1", claimForSegment(1, "/pvc/custom-size.yaml")),
        resource.desiredResources(scan, context));
    }

    @Test
    void doNotReturnClaimsForCompletedSegments() {
      verifyClaims(Map.of("0", claimForSegment(0), "1", claimForSegment(1)),
        resource.desiredResources(scan, context));

      DuplicateMessageScanStatus status = new DuplicateMessageScanStatus(spec.getSegments());
      scan.setStatus(status);

      verifyClaims(Map.of("0", claimForSegment(0), "1", claimForSegment(1)),
        resource.desiredResources(scan, context));

      status.segments.get(0).completed();
      verifyClaims(Map.of("1", claimForSegment(1)),
        resource.desiredResources(scan, context));
    }
  }

  @BeforeEach
  void beforeEach() {
    resource = new PersistentVolumeClaimDependentResource(configuration);

    scan = new DuplicateMessageScan(spec);
    scan.setMetadata(objectMeta);
  }

  private PersistentVolumeClaim claimForSegment(int index) {
    return claimForSegment(index, "/pvc/basic.yaml");
  }

  private PersistentVolumeClaim claimForSegment(int index, String yaml) {
    PersistentVolumeClaim base = loadYaml(PersistentVolumeClaim.class, getClass(), yaml);
    return new PersistentVolumeClaimBuilder(base)
      .withMetadata(new ObjectMetaBuilder(base.getMetadata())
        .withName(String.format("test-%d", index))
        .addToLabels(Constants.SEGMENT_LABEL_KEY, String.valueOf(index)).build())
      .build();
  }

  private void initClaims(Stream<PersistentVolumeClaim> claims) {
    when(context.getSecondaryResourcesAsStream(PersistentVolumeClaim.class)).thenReturn(claims);
  }

  private void verifyClaims(Map<String, PersistentVolumeClaim> expected, Map<String, PersistentVolumeClaim> actual) {
    assertEquals(expected.size(), actual.size());
    expected.forEach((key, claim) -> {
      assertTrue(actual.containsKey(key), String.format("PVC for key %1s not found, keys are: %1s", key,
        actual.keySet()));
      assertEqualsWithYaml(claim, actual.get(key));
    });
  }
}
