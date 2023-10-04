package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

public class ConfigMapProvider implements DesiredProvider<ConfigMap, DuplicateMessageScan> {

    @Override
    public ConfigMap desired(DuplicateMessageScan scan, Context<DuplicateMessageScan> context) {
        // TODO(panda, 8/29/23): how to pass more properties
        // TODO(panda, 8/29/23): how to pass a log4j
        ConfigMapBuilder builder = new ConfigMapBuilder()
          .withMetadata(scan.buildDependentObjectMetadata());
        scan.getSpec().buildInputs(true).forEach((key, input) -> builder.addToData(key, mapToString(input)));
        return builder.build();
    }

    private String mapToString(Map<String, String> map) {
        StringWriter writer = new StringWriter();
        PrintWriter out = new PrintWriter(writer);
        String format = "%1s=%2s%n";
        map.keySet().stream().sorted().forEach(key -> out.printf(format, key, map.get(key)));
        return writer.toString();
    }
}
