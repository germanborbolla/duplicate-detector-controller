package com.sumologic.duplicate.detector.controller.customresource;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.MoreObjects;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import io.javaoperatorsdk.operator.api.ObservedGenerationAwareStatus;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class DuplicateMessageScanStatus extends ObservedGenerationAwareStatus {


    private static final String DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ssX";

    @PrinterColumn(name = "SUCCESSFUL", priority = 0)
    public boolean successful = false;
    @PrinterColumn(name = "FAILED", priority = 0)
    public boolean failed = false;
    @PrinterColumn(name = "ERROR", priority = 0)
    public String error;
    @JsonPropertyDescription("Pairs of time range and customer that will be scanned independently")
    public List<Segment> segments;
    @PrinterColumn(name = "SEGMENTS", priority = 0)
    public int segmentCount;
    @PrinterColumn(name = "PENDING_SEGMENTS", priority = 1)
    public int pendingSegmentCount;
    @PrinterColumn(name = "PROCESSING_SEGMENTS", priority = 1)
    public int processingSegmentCount;
    @PrinterColumn(name = "COMPLETED_SEGMENTS", priority = 1)
    public int completedSegmentCount;
    @PrinterColumn(name = "FAILED_SEGMENTS", priority = 1)
    public int failedSegmentCount;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DATE_PATTERN)
    public Date scanStartedTime;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DATE_PATTERN)
    public Date lastUpdateTime;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DATE_PATTERN)
    public Date completionTime;

    public DuplicateMessageScanStatus() {
    }

    public DuplicateMessageScanStatus(List<Segment> segments) {
        this.segments = segments;
        updateSegmentsCounts();
    }

    public DuplicateMessageScanStatus(List<Segment> segments, Date scanStartedTime) {
        this(segments);
        this.scanStartedTime = scanStartedTime;
    }

    public void failed(String error) {
        this.successful = false;
        this.failed = true;
        this.segments.stream().filter(Segment::isPendingOrProcessing).forEach(Segment::failed);
        this.error = error;
        updateSegmentsCounts();
    }

    public void completed() {
        this.successful = true;
        this.failed = false;
        this.segments.stream().filter(Segment::isPendingOrProcessing).forEach(Segment::completed);
        this.error = null;
        updateSegmentsCounts();
    }

    public void updateSegmentsCounts() {
        segmentCount = segments.size();
        pendingSegmentCount = 0;
        processingSegmentCount = 0;
        completedSegmentCount = 0;
        failedSegmentCount = 0;
        segments.forEach(s -> {
            switch (s.status) {
                case PENDING:
                    pendingSegmentCount += 1;
                    break;
                case PROCESSING:
                    processingSegmentCount += 1;
                    break;
                case COMPLETED:
                    completedSegmentCount += 1;
                    break;
                case FAILED:
                    failedSegmentCount += 1;
                    break;
            }
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DuplicateMessageScanStatus that = (DuplicateMessageScanStatus) o;
        return successful == that.successful && failed == that.failed && Objects.equals(error, that.error) && Objects.equals(segments, that.segments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(successful, failed, error, segments);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
          .add("successful", successful)
          .add("failed", failed)
          .add("error", error)
          .add("segments", segments)
          .toString();
    }
}
