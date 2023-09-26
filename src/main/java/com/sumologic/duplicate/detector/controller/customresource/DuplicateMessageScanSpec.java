package com.sumologic.duplicate.detector.controller.customresource;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.MoreObjects;
import io.fabric8.generator.annotation.Min;
import io.fabric8.generator.annotation.Nullable;
import io.fabric8.generator.annotation.Required;

import java.util.*;

public class DuplicateMessageScanSpec {
  private static final String CUSTOMERS_KEY = "duplicate-detector.customers";
  private static final String START_TIME_KEY = "duplicate-detector.startTime";
  private static final String END_TIME_KEY = "duplicate-detector.endTime";
  private static final String TARGET_OBJECT_KEY = "duplicate_detector.targetObject";
  private static final String WORKING_DIR_KEY = "duplicate_detector.parentWorkingDir";
  private static final Map<String, String> DEFAULT_PROPERTIES = Map.of(
    "duplicate_detector.onExitInvoke", "pkill fluent-bit");
  private static final String DEFAULT_TARGET_OBJECT = "indices";

  private String startTime;
  private String endTime;
  private List<String> customers;
  private String volumeSize;
  private String targetObject;
  private int maxParallelScans = 1;

  public DuplicateMessageScanSpec() {
  }

  public DuplicateMessageScanSpec(String startTime, String endTime, List<String> customers) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.customers = customers;
  }

  @JsonPropertyDescription("Start time for the scan, in ISO format")
  @Required
  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  @JsonPropertyDescription("End time for the scan, in ISO format")
  @Required
  public String getEndTime() {
    return endTime;
  }

  public void setEndTime(String endTime) {
    this.endTime = endTime;
  }

  @JsonPropertyDescription("List of customers to scan")
  @Required
  public List<String> getCustomers() {
    return customers;
  }

  public void setCustomers(List<String> customers) {
    this.customers = customers;
  }

  @JsonPropertyDescription("[Optional] Size of the volume to attach to the scanning jobs, defaults to '300g'")
  @Nullable
  public String getVolumeSize() {
    return volumeSize;
  }

  public void setVolumeSize(String volumeSize) {
    this.volumeSize = volumeSize;
  }

  @JsonPropertyDescription("[Optional] What to scan either blocks or indices, defaults to 'indices'")
  @Nullable
  public String getTargetObject() {
    return targetObject;
  }

  public void setTargetObject(String targetObject) {
    this.targetObject = targetObject;
  }

  @JsonPropertyDescription("Max number of scans that can be performed at the same time, defaults to 1")
  @Min(1)
  public int getMaxParallelScans() {
    return maxParallelScans;
  }

  public void setMaxParallelScans(int maxParallelScans) {
    this.maxParallelScans = maxParallelScans;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DuplicateMessageScanSpec that = (DuplicateMessageScanSpec) o;
    return maxParallelScans == that.maxParallelScans && startTime.equals(that.startTime) && endTime.equals(that.endTime) && customers.equals(that.customers) && Objects.equals(volumeSize, that.volumeSize) && Objects.equals(targetObject, that.targetObject);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startTime, endTime, customers, volumeSize, targetObject, maxParallelScans);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("startTime", startTime)
      .add("endTime", endTime)
      .add("customers", customers)
      .add("volumeSize", volumeSize)
      .add("targetObject", targetObject)
      .add("maxParallelScans", maxParallelScans)
      .toString();
  }

  public Map<String, Map<String, String>> buildInputs() {
    if (customers.isEmpty()) {
      throw new IllegalArgumentException("We need some customers");
    } else if (customers.size() == 1) {
      return Map.of("duplicate_detector.properties",
        mapFor(customers.get(0), "/usr/sumo/system-tools/duplicate-detector-state"));
    } else {
      Map<String, Map<String, String>> inputs = new HashMap<>();
      for (int i = 0; i < customers.size(); i++) {
        inputs.put(String.format("duplicate_detector-%1d.properties", i),
          mapFor(customers.get(i), String.format("/usr/sumo/system-tools/duplicate-detector-state-%1d", i)));
      }
      return inputs;
    }
  }

  private Map<String, String> mapFor(String customers, String workingDir) {
    Map<String, String> map = new HashMap<>(DEFAULT_PROPERTIES);
    map.put(CUSTOMERS_KEY, customers);
    map.put(START_TIME_KEY, startTime);
    map.put(END_TIME_KEY, endTime);
    map.put(TARGET_OBJECT_KEY, Optional.ofNullable(targetObject).orElse(DEFAULT_TARGET_OBJECT));
    map.put(WORKING_DIR_KEY, workingDir);
    return map;
  }
}
