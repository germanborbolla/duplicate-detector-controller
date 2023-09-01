package com.sumologic.duplicate.detector.controller.customresource;

import com.google.common.base.MoreObjects;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;

import java.util.Objects;

public class SingleDuplicateMessageScanStatus {
    private boolean successful;
    private boolean completed;
    private String error;

    public SingleDuplicateMessageScanStatus() {
    }

    public SingleDuplicateMessageScanStatus(JobStatus jobStatus) {
        if (jobStatus.getSucceeded() > 0 || jobStatus.getFailed() > 0) {
            completed = true;
            successful = jobStatus.getSucceeded() > 0;
        } else {
            completed = false;
            successful = false;
        }
    }

    public SingleDuplicateMessageScanStatus(Exception e) {
        this.completed = false;
        this.successful = false;
        this.error = e.getMessage();
    }

    public boolean isSuccessful() {
        return successful;
    }

    public void setSuccessful(boolean successful) {
        this.successful = successful;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SingleDuplicateMessageScanStatus that = (SingleDuplicateMessageScanStatus) o;
        return successful == that.successful && completed == that.completed && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(successful, completed, error);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
          .add("successful", successful)
          .add("completed", completed)
          .add("error", error)
          .toString();
    }
}
