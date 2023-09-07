package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.Constants;
import com.sumologic.duplicate.detector.controller.customresource.SingleDuplicateMessageScan;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResourceConfig;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class ConfigMapDependantResource extends CRUDKubernetesDependentResource<ConfigMap, SingleDuplicateMessageScan> {

    public static KubernetesDependentResource<ConfigMap, SingleDuplicateMessageScan> create(KubernetesClient client) {
        ConfigMapDependantResource resource = new ConfigMapDependantResource();
        resource.setKubernetesClient(client);
        resource.configureWith(new KubernetesDependentResourceConfig<ConfigMap>()
          .setLabelSelector(Constants.RESOURCE_LABEL_SELECTOR));
        return resource;
    }

    private static final String TARGET_OBJECT_KEY = "duplicate_detector.targetObject";
    private static final Map<String, String> DEFAULT_PROPERTIES = Map.of(
      "duplicate_detector.onExitInvoke", "pkill fluent-bit",
      "duplicate_detector.parentWorkingDir", "/usr/sumo/system-tools/duplicate-detector-state");
    public ConfigMapDependantResource() {
        super(ConfigMap.class);
    }

    @Override
    protected ConfigMap desired(SingleDuplicateMessageScan scan, Context<SingleDuplicateMessageScan> context) {
        // TODO(panda, 8/29/23): how to pass more properties
        // TODO(panda, 8/29/23): how to pass a log4j
        Map<String, String> properties = new HashMap<>(DEFAULT_PROPERTIES);
        String targetObject = "indices";
        if (scan.getSpec().getTargetObject() != null) {
            targetObject = scan.getSpec().getTargetObject();
        }
        properties.put(TARGET_OBJECT_KEY, targetObject);
        return new ConfigMapBuilder()
          .withMetadata(scan.buildDependentObjectMetadata())
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
