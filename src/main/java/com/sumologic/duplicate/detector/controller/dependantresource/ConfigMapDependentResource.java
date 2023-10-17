package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.Constants;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import com.sumologic.duplicate.detector.controller.customresource.Segment;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResourceConfig;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ConfigMapDependentResource extends CRUDKubernetesDependentResource<ConfigMap, DuplicateMessageScan> {

  protected static final String CUSTOMERS_KEY = "duplicate_detector.customers";
  protected static final String START_TIME_KEY = "duplicate_detector.startTime";
  protected static final String END_TIME_KEY = "duplicate_detector.endTime";
  protected static final String TARGET_OBJECT_KEY = "duplicate_detector.targetObject";
  private static final Map.Entry<String, String> WORKING_DIR_ENTRY =
    Map.entry("duplicate_detector.parentWorkingDir", "/usr/sumo/system-tools/duplicate-detector-state");
  private static final Map.Entry<String, String> KILL_SIDECAR_ENTRY =
    Map.entry("duplicate_detector.onExitInvoke", "pkill fluent-bit");

  public static ConfigMapDependentResource create(KubernetesClient client) {
    ConfigMapDependentResource resource = new ConfigMapDependentResource();
    resource.setKubernetesClient(client);
    resource.configureWith(new KubernetesDependentResourceConfig<ConfigMap>()
      .setLabelSelector(Constants.RESOURCE_LABEL_SELECTOR));
    return resource;
  }
  public ConfigMapDependentResource() {
    super(ConfigMap.class);
  }

  @Override
  protected ConfigMap desired(DuplicateMessageScan scan, Context<DuplicateMessageScan> context) {
    // TODO(panda, 8/29/23): how to pass more properties
    // TODO(panda, 8/29/23): how to pass a log4j
    ConfigMapBuilder builder = new ConfigMapBuilder()
      .withMetadata(scan.buildDependentObjectMetadata());
    List<Segment> segments = scan.getSpec().getSegments();
    segments.forEach(segment -> builder.addToData(String.format("duplicate_detector-%s.properties", segment.id),
      propertiesForSegment(segment, scan.getSpec().targetObject)));
    return builder.build();
  }

  private String propertiesForSegment(Segment segment, String targetObject) {
    StringWriter writer = new StringWriter();
    PrintWriter out = new PrintWriter(writer);
    String format = "%1s=%2s%n";
    out.printf(format, CUSTOMERS_KEY, segment.customer);
    out.printf(format, START_TIME_KEY, segment.startTime);
    out.printf(format, END_TIME_KEY, segment.endTime);
    out.printf(format, TARGET_OBJECT_KEY, targetObject);
    out.printf(format, WORKING_DIR_ENTRY.getKey(), WORKING_DIR_ENTRY.getValue());
    out.printf(format, KILL_SIDECAR_ENTRY.getKey(), KILL_SIDECAR_ENTRY.getValue());
    return writer.toString();
  }
}
