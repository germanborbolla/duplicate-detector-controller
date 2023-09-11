package com.sumologic.duplicate.detector.controller;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.javaoperatorsdk.operator.Operator;
import org.apache.logging.log4j.core.config.Configurator;

import java.time.Duration;

public class Controller {
    public static void main(String[] args) {
        Configurator.initialize("config", null, "classpath:log4j2.xml");
        KubernetesClient client = new KubernetesClientBuilder().build();
        Operator operator = new Operator();
        operator.register(new SingleDuplicateMessageScanReconciler(client));
        // TODO(panda, 8/29/23): get duration from environment
        operator.installShutdownHook(Duration.ofMinutes(2));
        operator.start();
    }
}
