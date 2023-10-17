package com.sumologic.duplicate.detector.controller.customresource;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.MoreObjects;
import com.sumologic.duplicate.detector.controller.Constants;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import io.fabric8.generator.annotation.*;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DuplicateMessageScanSpec {
  @JsonPropertyDescription("Start time for the scan, in ISO format")
  @Required
  @PrinterColumn(name = "START_TIME", priority = 0)
  public String startTime;

  @JsonPropertyDescription("End time for the scan, in ISO format")
  @Required
  @PrinterColumn(name = "END_TIME", priority = 0)
  public String endTime;

  @JsonPropertyDescription("List of customers to scan")
  @Required
  public List<String> customers;

  @JsonPropertyDescription("Size of the volume to attach to the scanning jobs")
  public String volumeSize;

  @JsonPropertyDescription("What to scan either blocks or indices")
  @Default(Constants.DEFAULT_TARGET_OBJECT)
  public String targetObject = Constants.DEFAULT_TARGET_OBJECT;

  @JsonPropertyDescription("Max number of scans that can be performed at the same time")
  @Min(1)
  @Default("1")
  public int maxParallelScans = 1;

  @JsonPropertyDescription("Length of how long each scan should be, for example to break the full time range into 5m segments set this to PT5m")
  @Nullable
  public String timeRangeSegmentLength;

  @JsonPropertyDescription("How many times to attempt each segment")
  @Min(1)
  @Max(5)
  @Default("3")
  public int retriesPerSegment = 3;

  public DuplicateMessageScanSpec() {
  }

  public DuplicateMessageScanSpec(String startTime, String endTime, List<String> customers) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.customers = customers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DuplicateMessageScanSpec that = (DuplicateMessageScanSpec) o;
    return maxParallelScans == that.maxParallelScans && startTime.equals(that.startTime)
      && endTime.equals(that.endTime) && customers.equals(that.customers)
      && Objects.equals(volumeSize, that.volumeSize) && Objects.equals(targetObject, that.targetObject)
      && Objects.equals(timeRangeSegmentLength, that.timeRangeSegmentLength)
      && retriesPerSegment == that.retriesPerSegment;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startTime, endTime, customers, volumeSize, targetObject, maxParallelScans,
      timeRangeSegmentLength, retriesPerSegment);
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
      .add("timeRangeSegmentLength", timeRangeSegmentLength)
      .add("retriesPerSegment", retriesPerSegment)
      .toString();
  }

  public List<Segment> getSegments() {
    List<Pair<String, String>> scanningSegments = splitTimeRange();
    AtomicInteger id = new AtomicInteger();
    return customers.stream()
      .flatMap(customer -> scanningSegments.stream().map(interval ->
        new Segment(String.valueOf(id.getAndIncrement()), customer, interval.getLeft(), interval.getRight())))
      .collect(Collectors.toList());
  }

  protected List<Pair<String, String>> splitTimeRange() {
    if (timeRangeSegmentLength == null) {
      return List.of(Pair.of(startTime, endTime));
    } else {
      List<Pair<String, String>> segments = new LinkedList<>();
      Duration period = Duration.parse(timeRangeSegmentLength);
      Instant startTimeInstant = DateTimeFormatter.ISO_DATE_TIME.parse(startTime, Instant::from);
      Instant endTimeInstant = DateTimeFormatter.ISO_DATE_TIME.parse(endTime, Instant::from);
      Instant segmentEndTime = startTimeInstant.plus(period);
      while (segmentEndTime.isBefore(endTimeInstant)) {
        segments.add(Pair.of(startTimeInstant.toString(), segmentEndTime.toString()));
        startTimeInstant = segmentEndTime;
        segmentEndTime = startTimeInstant.plus(period);
      }
      segments.add(Pair.of(startTimeInstant.toString(), endTimeInstant.toString()));
      return segments;
    }
  }
}
