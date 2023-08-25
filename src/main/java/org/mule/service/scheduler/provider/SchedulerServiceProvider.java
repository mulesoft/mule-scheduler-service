/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.provider;

import org.mule.runtime.api.scheduler.SchedulerService;
import org.mule.runtime.api.service.ServiceDefinition;
import org.mule.runtime.api.service.ServiceProvider;
import org.mule.service.scheduler.internal.DefaultSchedulerService;

/**
 * Provides a definition for {@link SchedulerService}.
 *
 * @since 1.0
 */
public class SchedulerServiceProvider implements ServiceProvider {

  private final ServiceDefinition serviceDefinition =
      new ServiceDefinition(SchedulerService.class, new DefaultSchedulerService());

  @Override
  public ServiceDefinition getServiceDefinition() {
    return serviceDefinition;
  }
}
