package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanStatus;
import com.sumologic.duplicate.detector.controller.customresource.Segment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
public abstract class DuplicateMessageScanReconcilerTestBase {

  @Mock
  protected KubernetesClient client;

  @Mock
  protected Context<DuplicateMessageScan> context;

  protected ReconcilerConfiguration configuration = new ReconcilerConfiguration(
    new ReconcilerConfiguration.JobConfiguration("image:123", true,
      5, true),
    new ReconcilerConfiguration.PersistentVolumeConfiguration("gp2", "100Mi"));

  protected DuplicateMessageScanReconciler reconciler;

  protected void beforeEach() {
    reconciler = new DuplicateMessageScanReconciler(client, configuration);
  }

  protected void verifyResult(UpdateControl<DuplicateMessageScan> control, List<Segment> expectedSegments,
                            boolean expectedSuccessful, boolean expectedFailed, String expectedError) {
    verifyResult(control.getResource().getStatus(), expectedSegments, expectedSuccessful, expectedFailed,
      expectedError);
  }

  protected void verifyResult(DuplicateMessageScanStatus status, List<Segment> expectedSegments,
                            boolean expectedSuccessful, boolean expectedFailed, String expectedError) {
    assertNotNull(status);
    assertEquals(expectedSegments.size(), status.segmentCount);
    assertEquals(expectedSegments, status.segments);
    assertEquals(expectedError, status.error);
    assertEquals(expectedSuccessful, status.successful);
    assertEquals(expectedFailed, status.failed);
    assertEquals(expectedSegments.stream().filter(s -> s.status == Segment.SegmentStatus.PENDING).count(),
      status.pendingSegmentCount);
    assertEquals(expectedSegments.stream().filter(s -> s.status == Segment.SegmentStatus.PROCESSING).count(),
      status.processingSegmentCount);
    assertEquals(expectedSegments.stream().filter(s -> s.status == Segment.SegmentStatus.COMPLETED).count(),
      status.completedSegmentCount);
    assertEquals(expectedSegments.stream().filter(s -> s.status == Segment.SegmentStatus.FAILED).count(),
      status.failedSegmentCount);
  }

}
