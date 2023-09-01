package com.sumologic.duplicate.detector.controller.customresource;

import com.google.common.base.MoreObjects;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;

import java.util.Objects;

public class SingleDuplicateMessageScanStatus {

    private JobStatus jobStatus;
    private String error;

    public SingleDuplicateMessageScanStatus() {
    }

    public SingleDuplicateMessageScanStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
        this.error = null;
    }

    public SingleDuplicateMessageScanStatus(Exception e) {
        this.jobStatus = null;
        this.error = e.getMessage();
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public JobStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SingleDuplicateMessageScanStatus that = (SingleDuplicateMessageScanStatus) o;
        return Objects.equals(jobStatus, that.jobStatus) && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobStatus, error);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
          .add("jobStatus", jobStatus)
          .add("error", error)
          .toString();
    }
}
