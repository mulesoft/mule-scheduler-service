/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static org.mockito.Mockito.when;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.DEDICATED;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;

import static java.lang.System.lineSeparator;

import org.mule.runtime.api.scheduler.SchedulerPoolsConfig;

import java.util.OptionalInt;
import java.util.OptionalLong;

import io.qameta.allure.Feature;

@Feature(SCHEDULER_SERVICE)
public class DefaultSchedulerServiceTestCase extends SchedulerServiceContractTestCase {

  @Override
  protected String getCpuLightPrefix() {
    return "CPU_LIGHT - cpuLight";
  }

  @Override
  protected boolean areCpuLightTasksInWaitGroup() {
    return false;
  }

  @Override
  protected boolean areIoTasksInWaitGroup() {
    return true;
  }

  @Override
  protected boolean areCpuLightTasksInCpuWorkGroup() {
    return true;
  }

  @Override
  protected boolean areIoTasksInCpuWorkGroup() {
    return false;
  }

  @Override
  protected String getSplashMessage() {
    return "cpuLight.threadPool.size:      " +
        config.getCpuLightPoolSize().getAsInt() + lineSeparator() +
        "cpuLight.workQueue.size:       " +
        config.getCpuLightQueueSize().getAsInt() + lineSeparator() +
        "io.threadPool.maxSize:         " +
        config.getIoMaxPoolSize().getAsInt() + lineSeparator() +
        "io.threadPool.threadKeepAlive: " +
        config.getIoKeepAlive().getAsLong() + " ms" + lineSeparator() +
        "cpuIntensive.threadPool.size:  " +
        config.getCpuIntensivePoolSize().getAsInt() + lineSeparator() +
        "cpuIntensive.workQueue.size:   " +
        config.getCpuIntensiveQueueSize().getAsInt() + lineSeparator();
  }

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
