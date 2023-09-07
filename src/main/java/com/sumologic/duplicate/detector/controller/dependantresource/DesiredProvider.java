package com.sumologic.duplicate.detector.controller.dependantresource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.Context;

public interface DesiredProvider<R extends HasMetadata, P extends HasMetadata> {
  R desired(P custom, Context<P> context);
}
