/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
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
