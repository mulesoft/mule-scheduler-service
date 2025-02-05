/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.mule.runtime.api.scheduler.SchedulerService;
import org.mule.service.scheduler.internal.service.DefaultSchedulerService;
import org.mule.service.scheduler.provider.SchedulerServiceProvider;

import io.qameta.allure.Feature;
import org.junit.Test;

@Feature(SCHEDULER_SERVICE)
public class SchedulerServiceProviderTestCase {

  @Test
  public void provider() {
    SchedulerServiceProvider provider = new SchedulerServiceProvider();

    assertThat(provider.getServiceDefinition().getServiceClass(), is(SchedulerService.class));
    assertThat(provider.getServiceDefinition().getService(), is(instanceOf(DefaultSchedulerService.class)));
  }

}
