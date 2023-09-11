package com.sumologic.duplicate.detector.controller.dependantresource;

import com.sumologic.duplicate.detector.controller.Constants;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependentResourceConfig;

public class ProviderKubernetesDependentResource<R extends HasMetadata, P extends HasMetadata> extends CRUDKubernetesDependentResource<R, P> {
  public static <R extends HasMetadata, P extends HasMetadata> ProviderKubernetesDependentResource<R, P> create(
    Class<R> resourceType, DesiredProvider<R, P> desiredProvider, KubernetesClient client) {
    ProviderKubernetesDependentResource<R, P> resource = new ProviderKubernetesDependentResource<>(resourceType, desiredProvider);
    resource.setKubernetesClient(client);
    resource.configureWith(new KubernetesDependentResourceConfig<R>()
      .setLabelSelector(Constants.RESOURCE_LABEL_SELECTOR));
    return resource;
  }
  private DesiredProvider<R, P> desiredProvider;

  public ProviderKubernetesDependentResource(Class<R> resourceType, DesiredProvider<R, P> desiredProvider) {
    super(resourceType);
    this.desiredProvider = desiredProvider;
  }

  @Override
  protected R desired(P primary, Context<P> context) {
    return desiredProvider.desired(primary, context);
  }
}
