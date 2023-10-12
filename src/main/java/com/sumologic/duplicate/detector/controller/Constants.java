package com.sumologic.duplicate.detector.controller;

public class Constants {
    public static final String OPERATOR_NAME = "duplicate-detector-operator";

    public static final String RESOURCE_LABEL_SELECTOR = "app.kubernetes.io/managed-by=" + OPERATOR_NAME;

    public static final String SYSTEM_TOOLS_IMAGE_ENV_NAME = "SYSTEM_TOOLS_IMAGE";

    public static final String KILL_TAILING_SIDECARS_ENV_NAME = "KILL_TAILING_SIDECARS";

    public static final String SEGMENT_LABEL_KEY = "duplicatedetector.sumologic.com/segment-index";

    public static final String JOB_COMPLETION_INDEX_ENV_NAME = "JOB_COMPLETION_INDEX";
}
