/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.lang.Thread.sleep;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mule.runtime.api.scheduler.SchedulerConfig.config;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.loadThreadPoolsConfig;

import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.util.concurrent.Latch;
import org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig;
import org.mule.service.scheduler.internal.executor.SchedulerTaskThrottledException;
import org.mule.service.scheduler.internal.threads.SchedulerThreadPools;
import org.mule.tck.probe.JUnitLambdaProbe;
import org.mule.tck.probe.PollingProber;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;

@Feature("Scheduler Throttling")
public class ThrottledSchedulerThrottleTestCase extends BaseDefaultSchedulerTestCase {

  private static final int THROTTLE_SIZE = 2;
  private static final int SINGLE_TASK_THROTTLE_SIZE = 1;
  private ExecutorService outerExecutor;
  private ContainerThreadPoolsConfig threadPoolsConfig;
  private SchedulerThreadPools service;

  @Override
  @Before
  public void before() throws Exception {
    super.before();
    outerExecutor = newSingleThreadExecutor();

    threadPoolsConfig = loadThreadPoolsConfig();
    service = new SchedulerThreadPools(SchedulerThreadPoolsTestCase.class.getName(), threadPoolsConfig);
    service.start();
  }

  @Override
  @After
  public void after() throws Exception {
    if (service == null) {
      return;
    }
    for (Scheduler scheduler : new ArrayList<>(service.getSchedulers())) {
      scheduler.stop();
    }
    service.stop();

    outerExecutor.shutdownNow();
    outerExecutor.awaitTermination(5, SECONDS);
    super.after();
  }

  @Test
  @Description("Tests that a 'maxConcurrentTasks=1' configuration allows to execute a single task")
  public void oneConcurrentTaskSupported() throws InterruptedException {
    final ScheduledExecutorService scheduler = service
        .createIoScheduler(config().withMaxConcurrentTasks(SINGLE_TASK_THROTTLE_SIZE), SINGLE_TASK_THROTTLE_SIZE, () -> 5000L);
    Latch latch = new Latch();
    scheduler.submit(() -> latch.countDown());
    if (!latch.await(200, MILLISECONDS)) {
      fail("Task never executed");
    }
  }

  @Test
  @Description("Tests that a task submitted in excess of 'maxConcurrentTasks' waits until another task finishes before executing.")
  public void throttledTask() throws InterruptedException {
    final ScheduledExecutorService scheduler =
        service.createIoScheduler(config().withMaxConcurrentTasks(THROTTLE_SIZE), THROTTLE_SIZE, () -> 5000L);
    final Latch latch = new Latch();

    for (int i = 0; i < THROTTLE_SIZE; ++i) {
      scheduler.execute(() -> {
        awaitLatch(latch);
      });
    }

    final Future<?> throttledSubmission = outerExecutor.submit(() -> {
      scheduler.execute(() -> {
        // Nothing to do
      });
    });

    sleep(10);
    assertThat(throttledSubmission.isDone(), is(false));

    latch.countDown();

    new PollingProber(100, 10).check(new JUnitLambdaProbe(() -> {
      assertThat(throttledSubmission.isDone(), is(true));
      return true;
    }));
  }

  @Test
  @Description("Tests that a task submitted in excess of 'maxConcurrentTasks' is rejected when called from a cpu-processing thread.")
  public void throttledTaskRejected() throws InterruptedException, ExecutionException, TimeoutException {
    final ScheduledExecutorService scheduler = service
        .createIoScheduler(config().withMaxConcurrentTasks(SINGLE_TASK_THROTTLE_SIZE), SINGLE_TASK_THROTTLE_SIZE, () -> 5000L);
    final Latch latch = new Latch();

    for (int i = 0; i < THROTTLE_SIZE; ++i) {
      scheduler.execute(() -> {
        awaitLatch(latch);
      });
    }

    Scheduler cpuLightScheduler = service.createCpuLightScheduler(config(), 2, () -> 5000L);

    Future<?> submittedTest = cpuLightScheduler.submit(() -> {
      try {
        scheduler.execute(() -> {
          // Nothing to do
        });
        fail("Expected the task to be rejected with a 'SchedulerTaskThrottledException'");
      } catch (SchedulerTaskThrottledException rejected) {
        // Nothing to do
      }
    });

    submittedTest.get(1, SECONDS);
  }

}
