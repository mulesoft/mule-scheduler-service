/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.provider;

import org.mule.runtime.api.scheduler.SchedulerService;
import org.mule.runtime.api.service.ServiceDefinition;
import org.mule.runtime.api.service.ServiceProvider;
import org.mule.service.scheduler.internal.SingleIOPoolSchedulerService;

/**
 * Provides a definition for {@link SchedulerService}.
 *
 * @since 1.0
 */
public class SchedulerServiceProvider implements ServiceProvider {

  private final ServiceDefinition serviceDefinition =
      new ServiceDefinition(SchedulerService.class, new SingleIOPoolSchedulerService());

  @Override
  public ServiceDefinition getServiceDefinition() {
    return serviceDefinition;
  }
}
