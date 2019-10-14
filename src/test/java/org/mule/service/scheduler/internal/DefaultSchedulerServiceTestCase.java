/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static org.mockito.Mockito.when;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.DEDICATED;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;

import org.mule.runtime.api.scheduler.SchedulerPoolsConfig;

import java.util.OptionalInt;
import java.util.OptionalLong;

import io.qameta.allure.Feature;

@Feature(SCHEDULER_SERVICE)
public class DefaultSchedulerServiceTestCase extends SchedulerServiceContractTestCase {

  @Override
  protected void configure(SchedulerPoolsConfig config) {
    when(config.getSchedulerPoolStrategy()).thenReturn(DEDICATED);
    when(config.getCpuLightPoolSize()).thenReturn(OptionalInt.of(1));
    when(config.getCpuLightQueueSize()).thenReturn(OptionalInt.of(1));
    when(config.getCpuIntensiveQueueSize()).thenReturn(OptionalInt.of(1));
    when(config.getIoCorePoolSize()).thenReturn(OptionalInt.of(1));
    when(config.getIoMaxPoolSize()).thenReturn(OptionalInt.of(1));
    when(config.getIoQueueSize()).thenReturn(OptionalInt.of(0));
    when(config.getIoKeepAlive()).thenReturn(OptionalLong.of(30000L));
    when(config.getCpuIntensivePoolSize()).thenReturn(OptionalInt.of(1));
    when(config.getCpuIntensiveQueueSize()).thenReturn(OptionalInt.of(1));
  }
}
