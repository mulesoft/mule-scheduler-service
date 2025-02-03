/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static org.mule.runtime.api.scheduler.SchedulerConfig.config;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.UBER;

import org.mule.runtime.api.scheduler.Scheduler;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

import io.qameta.allure.Description;
import io.qameta.allure.Issue;

public class UberSchedulerThreadPoolsTestCase extends SchedulerThreadPoolsTestCase {

  public UberSchedulerThreadPoolsTestCase() {
    this.strategy = UBER;
  }

  @Test
  @Issue("MULE-20072")
  @Description("Tests that when a rejected task submitted from a CPU Intensive pool is executed on the caller thread, the thread locals of the task are isolated from the caller's.")
  // Assumes Uber strategy, because on Dedicated, tasks submitted from a CPU Intensive pool to a busy pool are rejected.
  public void cpuIntensiveCallerRunsHasThreadLocalsIsolation() throws ExecutionException, InterruptedException {
    Scheduler scheduler = service.createCpuIntensiveScheduler(config(), CORES, () -> 1000L);
    int maxPoolSize = threadPoolsConfig.getUberMaxPoolSize().getAsInt();
    assertCallerRunsThreadLocalsIsolation(scheduler, maxPoolSize);
  }

}
