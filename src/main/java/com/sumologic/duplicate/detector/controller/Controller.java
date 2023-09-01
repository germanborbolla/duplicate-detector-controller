package com.sumologic.duplicate.detector.controller;

import io.javaoperatorsdk.operator.Operator;
import org.apache.logging.log4j.core.config.Configurator;

import java.time.Duration;

public class Controller {
    public static void main(String[] args) {
        Configurator.initialize("config", null, "classpath:log4j2.xml");
        Operator operator = new Operator();
        operator.register(new DuplicateMessageScanReconciler());
        // TODO(panda, 8/29/23): get duration from environment
        operator.installShutdownHook(Duration.ofMinutes(2));
        operator.start();
    }
}
