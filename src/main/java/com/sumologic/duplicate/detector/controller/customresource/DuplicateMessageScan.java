package com.sumologic.duplicate.detector.controller.customresource;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

import static com.sumologic.duplicate.detector.controller.Constants.OPERATOR_NAME;

@Group("com.sumologic.duplicate.detector")
@Version("v1")
@ShortNames("dms")
public class DuplicateMessageScan
  extends CustomResource<DuplicateMessageScanSpec, DuplicateMessageScanStatus> implements Namespaced {

  public DuplicateMessageScan() {
  }

  public DuplicateMessageScan(DuplicateMessageScanSpec spec) {
    this.spec = spec;
  }

  public DuplicateMessageScan withStatus(DuplicateMessageScanStatus status) {
    this.status = status;
    return this;
  }

  public ObjectMeta buildDependentObjectMetadata() {
    return new ObjectMetaBuilder()
      .withName(getMetadata().getName())
      .withNamespace(getMetadata().getNamespace())
      .addToLabels(getMetadata().getLabels())
      .addToLabels("app.kubernetes.io/managed-by", OPERATOR_NAME)
      .addToLabels("app.kubernetes.io/part-of", getMetadata().getName()).
      build();
  }
}
