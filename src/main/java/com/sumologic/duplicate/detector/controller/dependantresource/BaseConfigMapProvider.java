package com.sumologic.duplicate.detector.controller.dependantresource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BaseConfigMapProvider {
  private static final String TARGET_OBJECT_KEY = "duplicate_detector.targetObject";
  private static final Map<String, String> DEFAULT_PROPERTIES = Map.of(
    "duplicate_detector.onExitInvoke", "pkill fluent-bit",
    "duplicate_detector.parentWorkingDir", "/usr/sumo/system-tools/duplicate-detector-state");

  protected ConfigMap desiredConfigMap(Optional<String> targetObject,
                                       Map<String, String> extraProperties,
                                       ObjectMeta objectMeta) {
    Map<String, String> properties = new HashMap<>(DEFAULT_PROPERTIES);
    properties.put(TARGET_OBJECT_KEY, targetObject.orElse("indices"));
    properties.putAll(extraProperties);
    return new ConfigMapBuilder()
      .withMetadata(objectMeta)
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
