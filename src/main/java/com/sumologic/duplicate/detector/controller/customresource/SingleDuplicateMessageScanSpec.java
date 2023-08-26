package com.sumologic.duplicate.detector.controller.customresource;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.MoreObjects;
import io.fabric8.generator.annotation.Nullable;
import io.fabric8.generator.annotation.Required;

import java.util.Objects;
import java.util.Optional;

public class SingleDuplicateMessageScanSpec {
    private String startTime;
    private String endTime;
    private String customer;
    private String volumeSize;
    private String targetObject;

    public SingleDuplicateMessageScanSpec() {
    }

    public SingleDuplicateMessageScanSpec(String startTime, String endTime, String customer) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.customer = customer;
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
    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @JsonPropertyDescription("[Optional] Size of the volume to attach to the scanning jobs, defaults to '300g'")
    @Nullable
    public Optional<String> getVolumeSize() {
        return Optional.ofNullable(volumeSize);
    }

    public SingleDuplicateMessageScanSpec setVolumeSize(String volumeSize) {
        this.volumeSize = volumeSize;
        return this;
    }

    @JsonPropertyDescription("[Optional] What to scan either blocks or indices, defaults to 'indices'")
    @Nullable
    public Optional<String> getTargetObject() {
        return Optional.ofNullable(targetObject);
    }

    public SingleDuplicateMessageScanSpec setTargetObject(String targetObject) {
        this.targetObject = targetObject;
        return this;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
          .add("startTime", startTime)
          .add("endTime", endTime)
          .add("customer", customer)
          .add("volumeSize", volumeSize)
          .add("targetObject", targetObject)
          .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SingleDuplicateMessageScanSpec that = (SingleDuplicateMessageScanSpec) o;
        return startTime.equals(that.startTime) && endTime.equals(that.endTime) && customer.equals(that.customer)
          && Objects.equals(volumeSize, that.volumeSize) && Objects.equals(targetObject, that.targetObject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime, endTime, customer, volumeSize, targetObject);
    }
}
