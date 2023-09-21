package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class ConfigMapProvider<R extends HasMetadata> implements DesiredProvider<ConfigMap, R> {
    private static final String TARGET_OBJECT_KEY = "duplicate_detector.targetObject";
    private static final String CUSTOMERS_KEY = "duplicate-detector.customers";
    private static final String START_TIME_KEY = "duplicate-detector.startTime";
    private static final String END_TIME_KEY = "duplicate-detector.endTime";

    public static ConfigMapProvider<SingleDuplicateMessageScan> createForSingleDuplicateMessageScan() {
        return new ConfigMapProvider<>(
          SingleDuplicateMessageScan::buildDependentObjectMetadata,
          scan -> Map.of(
            TARGET_OBJECT_KEY, Optional.ofNullable(scan.getSpec().getTargetObject()).orElse("indices"),
            CUSTOMERS_KEY, scan.getSpec().getCustomer(),
            START_TIME_KEY, scan.getSpec().getStartTime(),
            END_TIME_KEY, scan.getSpec().getEndTime())
        );
    }

    private static final Map<String, String> DEFAULT_PROPERTIES = Map.of(
      "duplicate_detector.onExitInvoke", "pkill fluent-bit",
      "duplicate_detector.parentWorkingDir", "/usr/sumo/system-tools/duplicate-detector-state");

    private final Function<R, Map<String, String>> mapFunction;
    private final Function<R, ObjectMeta> metadataFunction;

    public ConfigMapProvider(Function<R, ObjectMeta> metadataFunction,
                                  Function<R, Map<String, String>> mapFunction) {
        this.mapFunction = mapFunction;
        this.metadataFunction = metadataFunction;
    }

    @Override
    public ConfigMap desired(R scan, Context<R> context) {
        // TODO(panda, 8/29/23): how to pass more properties
        // TODO(panda, 8/29/23): how to pass a log4j
        Map<String, String> properties = new HashMap<>(DEFAULT_PROPERTIES);
        properties.putAll(mapFunction.apply(scan));
        return new ConfigMapBuilder()
          .withMetadata(metadataFunction.apply(scan))
          .addToData("duplicate_detector.properties", mapToString(properties))
          .build();
    }

    private String mapToString(Map<String, String> map) {
        StringWriter writer = new StringWriter();
        PrintWriter out = new PrintWriter(writer);
        String format = "%1s=%2s%n";
        map.keySet().stream().sorted().forEach(key -> out.printf(format, key, map.get(key)));
        return writer.toString();
    }
}
