package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.Constants;
import com.sumologic.duplicate.detector.controller.customresource.DuplicateMessageScan;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractBulkDependentResource<R extends HasMetadata> extends CRUDKubernetesDependentResource<R, DuplicateMessageScan> implements BulkDependentResource<R, DuplicateMessageScan> {
  private final Class<R> resourceType;

  public AbstractBulkDependentResource(Class<R> resourceType) {
    super(resourceType);
    this.resourceType = resourceType;
  }

  @Override
  public Map<String, R> getSecondaryResources(DuplicateMessageScan primary, Context<DuplicateMessageScan> context) {
    return context.getSecondaryResourcesAsStream(resourceType)
      .filter(job -> job.getMetadata() != null && job.getMetadata().getLabels() != null
        && job.getMetadata().getLabels().containsKey(Constants.SEGMENT_LABEL_KEY))
      .collect(Collectors.toMap(job -> job.getMetadata().getLabels().get(Constants.SEGMENT_LABEL_KEY),
        Function.identity()));
  }
}
