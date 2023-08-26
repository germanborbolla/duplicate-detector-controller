package com.sumologic.duplicate.detector.controller.customresource;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("com.sumologic.duplicate.detector")
@Version("v1")
@ShortNames("sdms")
public class SingleDuplicateMessageScan extends CustomResource<SingleDuplicateMessageScanSpec, SingleDuplicateMessageScanStatus>
  implements Namespaced {
  public SingleDuplicateMessageScan() {
  }

  public SingleDuplicateMessageScan(SingleDuplicateMessageScanSpec spec) {
    this.spec = spec;
  }

  public SingleDuplicateMessageScan withStatus(SingleDuplicateMessageScanStatus status) {
    this.status = status;
    return this;
  }
}
