/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static org.mule.runtime.core.api.config.MuleProperties.OBJECT_SCHEDULER_BASE_CONFIG;

import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerConfig;
import org.mule.runtime.api.scheduler.SchedulerPoolsConfig;
import org.mule.runtime.api.scheduler.SchedulerPoolsConfigFactory;
import org.mule.service.scheduler.internal.threads.SchedulerThreadPools;

import javax.inject.Inject;
import javax.inject.Named;

public class SingleIOPoolSchedulerService extends DefaultSchedulerService {

  @Override
  public Scheduler cpuLightScheduler() {
    return ioScheduler();
  }

  @Override
  public Scheduler cpuIntensiveScheduler() {
    return ioScheduler();
  }

  @Override
  public Scheduler cpuLightScheduler(SchedulerConfig config) {
    return ioScheduler(config);
  }

  @Override
  public Scheduler cpuIntensiveScheduler(SchedulerConfig config) {
    return ioScheduler(config);
  }

  @Override
  @Inject
  public Scheduler cpuLightScheduler(@Named(OBJECT_SCHEDULER_BASE_CONFIG) SchedulerConfig config,
                                     SchedulerPoolsConfigFactory poolsConfigFactory) {
    return ioScheduler(config, poolsConfigFactory);
  }

  @Override
  @Inject
  public Scheduler cpuIntensiveScheduler(@Named(OBJECT_SCHEDULER_BASE_CONFIG) SchedulerConfig config,
                                         SchedulerPoolsConfigFactory poolsConfigFactory) {
    return ioScheduler(config, poolsConfigFactory);
  }

  @Override
  protected SchedulerThreadPools createSchedulerThreadPools(String name, SchedulerPoolsConfig threadPoolsConfig) {
    return SchedulerThreadPools.builder()
        .setName(name)
        .setThreadPoolsConfig(threadPoolsConfig)
        .setTraceLogger(traceLogger)
        .preStartCpuLight(false)
        .preStartIO(true)
        .preStartCpuIntensive(false)
        .build();
  }

  @Override
  protected void describeCpuLightPoolForSplash(StringBuilder splashMessage) {
  }

  @Override
  protected void describeCpuIntensivePoolForSplash(StringBuilder splashMessage) {
  }
}
