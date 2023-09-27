package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScanSpec;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;

import java.util.List;

public class BaseTests {
  protected ObjectMeta objectMeta = new ObjectMetaBuilder()
    .withName("test").withNamespace("test").addToLabels("mykey", "hello").build();

  protected DuplicateMessageScan createSingleCustomerScan() { return createScan(createSingleCustomerSpec()); }
  protected DuplicateMessageScan createMultipleCustomerScan() {
    return createScan(createMultipleCustomerSpec());
  }

  protected DuplicateMessageScan createScan(DuplicateMessageScanSpec spec) {
    DuplicateMessageScan scan = new DuplicateMessageScan(spec);
    scan.setMetadata(objectMeta);
    return scan;
  }

  protected DuplicateMessageScanSpec createSingleCustomerSpec() {
    return new DuplicateMessageScanSpec("2023-09-06T10:00:00-07:00", "2023-09-06T10:15:00-07:00",
      List.of("0000000000000005"));
  }

  protected DuplicateMessageScanSpec createMultipleCustomerSpec() {
    return new DuplicateMessageScanSpec("2023-09-06T10:00:00-07:00", "2023-09-06T10:15:00-07:00",
      List.of("0000000000000005", "0000000000000006", "0000000000000007"));
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
