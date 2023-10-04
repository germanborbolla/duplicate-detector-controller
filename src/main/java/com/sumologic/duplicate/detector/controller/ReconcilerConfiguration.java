package com.sumologic.duplicate.detector.controller;

import java.util.Objects;
import java.util.Optional;

public class ReconcilerConfiguration {
  JobConfiguration jobConfiguration;
  PersistentVolumeConfiguration persistentVolumeConfiguration;

  public ReconcilerConfiguration() {
    this(new JobConfiguration(), new PersistentVolumeConfiguration());
  }

  public ReconcilerConfiguration(JobConfiguration jobConfiguration,
                                 PersistentVolumeConfiguration persistentVolumeConfiguration) {
    this.jobConfiguration = jobConfiguration;
    this.persistentVolumeConfiguration = persistentVolumeConfiguration;
  }

  public static class JobConfiguration {
    private final String systemToolsImage;
    private final boolean useIntegrationTestJob;
    private final int sleepTimeForIntegrationTest;

    private final boolean killTailingSidecars;

    public JobConfiguration() {
      this(Objects.requireNonNull(System.getenv(Constants.SYSTEM_TOOLS_IMAGE_ENV_NAME),
        "System tools image env variable not set"), false, 0,
        Optional.ofNullable(System.getenv(Constants.KILL_TAILING_SIDECARS_ENV_NAME))
          .map(Boolean::parseBoolean).orElse(false));
    }

    public JobConfiguration(String systemToolsImage, boolean useIntegrationTestJob, int sleepForIntegrationTest,
                            boolean killTailingSidecars) {
      this.systemToolsImage = systemToolsImage;
      this.useIntegrationTestJob = useIntegrationTestJob;
      this.sleepTimeForIntegrationTest = sleepForIntegrationTest;
      this.killTailingSidecars = killTailingSidecars;
    }

    public String getSystemToolsImage() {
      return systemToolsImage;
    }

    public boolean isUseIntegrationTestJob() {
      return useIntegrationTestJob;
    }

    public int getSleepTimeForIntegrationTest() {
      return sleepTimeForIntegrationTest;
    }

    public boolean isKillTailingSidecars() {
      return killTailingSidecars;
    }
  }

  public static class PersistentVolumeConfiguration {
    private String defaultStorageClassName;
    private final String defaultSize;

    public PersistentVolumeConfiguration() {
      this(Optional.ofNullable(System.getenv("PVC_STORAGE_CLASS_NAME")).orElse("gp2"),
        Optional.ofNullable(System.getenv("PVC_DEFAULT_SIZE")).orElse("300Gi"));
    }

    public PersistentVolumeConfiguration(String defaultStorageClassName, String defaultSize) {
      this.defaultStorageClassName = defaultStorageClassName;
      this.defaultSize = defaultSize;
    }

    public String getDefaultStorageClassName() {
      return defaultStorageClassName;
    }

    public void setDefaultStorageClassName(String defaultStorageClassName) {
      this.defaultStorageClassName = defaultStorageClassName;
    }

    public String getDefaultSize() {
      return defaultSize;
    }
  }
}
