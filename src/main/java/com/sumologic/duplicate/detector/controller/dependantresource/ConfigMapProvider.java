package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.Map;
import java.util.Optional;

public class ConfigMapProvider extends BaseConfigMapProvider implements DesiredProvider<ConfigMap, SingleDuplicateMessageScan> {

    public ConfigMapProvider() {
    }

    @Override
    public ConfigMap desired(SingleDuplicateMessageScan scan, Context<SingleDuplicateMessageScan> context) {
        // TODO(panda, 8/29/23): how to pass more properties
        // TODO(panda, 8/29/23): how to pass a log4j
        return desiredConfigMap(Optional.ofNullable(scan.getSpec().getTargetObject()),
          Map.of(),
          scan.buildDependentObjectMetadata());
    }

}
