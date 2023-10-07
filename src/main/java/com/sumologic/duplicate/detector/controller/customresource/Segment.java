package com.sumologic.duplicate.detector.controller.customresource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.MoreObjects;

import java.util.Objects;

public class Segment {

  public enum SegmentStatus {
    PENDING, PROCESSING, COMPLETED, FAILED
  }
  public String customer;
  public String startTime;
  public String endTime;

  public SegmentStatus status;

  public Segment(String customer, String startTime, String endTime) {
    this.customer = customer;
    this.startTime = startTime;
    this.endTime = endTime;
    this.status = SegmentStatus.PENDING;
  }

  public Segment() {
  }

  @JsonIgnore
  public boolean isPendingOrProcessing() {
    return status == SegmentStatus.PENDING || status == SegmentStatus.PROCESSING;
  }

  public void processing() {
    status = SegmentStatus.PROCESSING;
  }

  public void completed() {
    status = SegmentStatus.COMPLETED;
  }

  public void failed() {
    status = SegmentStatus.FAILED;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Segment segment = (Segment) o;
    return Objects.equals(customer, segment.customer) && Objects.equals(startTime, segment.startTime) && Objects.equals(endTime, segment.endTime) && status == segment.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(customer, startTime, endTime, status);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("customer", customer)
      .add("startTime", startTime)
      .add("endTime", endTime)
      .add("status", status)
      .toString();
  }
}
