package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScanSpec;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BaseTests {
  protected ObjectMeta objectMeta = new ObjectMetaBuilder()
    .withName("test").withNamespace("test").addToLabels("mykey", "hello").build();

  protected SingleDuplicateMessageScan createScan() {
    return createScan(new SingleDuplicateMessageScanSpec());
  }

  protected SingleDuplicateMessageScan createScan(SingleDuplicateMessageScanSpec spec) {
    SingleDuplicateMessageScan scan = new SingleDuplicateMessageScan(spec);
    scan.setMetadata(objectMeta);
    return scan;
  }

  protected <T> void assertEqualsWithYaml(T expected, T actual) {
    Assertions.assertEquals(expected, actual,
      () -> {
        String expectedAsString = Serialization.asYaml(expected);
        String actualAsString = Serialization.asYaml(actual);
        return String.format("Objects did not match, expected: %n%1s, actual: %n%2s, difference: %n%3s",
          expectedAsString, actualAsString, StringUtils.difference(expectedAsString, actualAsString));
      });
  }
}
