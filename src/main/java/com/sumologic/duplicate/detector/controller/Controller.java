package com.sumologic.duplicate.detector.controller;

import io.javaoperatorsdk.operator.Operator;

import java.time.Duration;

public class Controller {
    public static void main(String[] args) {
        // TODO(panda, 8/29/23): Configure logging
        Operator operator = new Operator();
        operator.register(new DuplicateMessageScanReconciler());
        // TODO(panda, 8/29/23): get duration from environment
        operator.installShutdownHook(Duration.ofMinutes(2));
        operator.start();
    }
}
