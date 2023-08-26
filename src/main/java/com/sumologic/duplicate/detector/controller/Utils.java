package com.sumologic.duplicate.detector.controller;

import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;

public class Utils {
    public static ObjectMeta buildMetadata(SingleDuplicateMessageScan scan) {
        return new ObjectMetaBuilder()
          .withName(scan.getMetadata().getName())
          .withNamespace(scan.getMetadata().getNamespace())
          .addToLabels(scan.getMetadata().getLabels())
          .addToLabels("app.kubernetes.io/managed-by", OPERATOR_NAME)
          .addToLabels("app.kubernetes.io/part-of", scan.getMetadata().getName()).
          build();
    }

    public static final String OPERATOR_NAME = "duplicate-detector-operator";

    public static final String RESOURCE_LABEL_SELECTOR = "app.kubernetes.io/managed-by=" + OPERATOR_NAME;

    public static final String SYSTEM_TOOLS_IMAGE_ENV_NAME = "SYSTEM_TOOLS_IMAGE";
}
