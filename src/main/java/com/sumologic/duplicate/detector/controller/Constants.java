package com.sumologic.duplicate.detector.controller;

public class Constants {
    public static final String OPERATOR_NAME = "duplicate-detector-operator";

    public static final String RESOURCE_LABEL_SELECTOR = "app.kubernetes.io/managed-by=" + OPERATOR_NAME;

    public static final String SYSTEM_TOOLS_IMAGE_ENV_NAME = "SYSTEM_TOOLS_IMAGE";
}
