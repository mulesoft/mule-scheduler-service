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
import static org.mule.service.scheduler.ThreadType.CUSTOM;
import org.mule.runtime.api.util.concurrent.Latch;
import org.mule.tck.probe.JUnitLambdaProbe;
import org.mule.tck.probe.PollingProber;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.SchedulerException;

@Feature("Scheduler Throttling")
public class ThrottledSchedulerThrottleTestCase extends BaseDefaultSchedulerTestCase {

  private static final int THROTTLE_SIZE = 2;
  private static final int SINGLE_TASK_THROTTLE_SIZE = 1;
  private ExecutorService outerExecutor;

  @Override
  @Before
  public void before() throws SchedulerException {
    super.before();
    outerExecutor = newSingleThreadExecutor();
  }

  @Override
  @After
  public void after() throws SchedulerException, InterruptedException {
    outerExecutor.shutdownNow();
    outerExecutor.awaitTermination(5, SECONDS);
    super.after();
  }

  @Test
  @Description("Tests that a 'maxConcurrentTasks=1' configuration allows to execute a single task")
  public void oneConcurrentTaskSupported() throws InterruptedException {
    final ScheduledExecutorService scheduler = createExecutor(SINGLE_TASK_THROTTLE_SIZE);
    Latch latch = new Latch();
    scheduler.submit(() -> latch.countDown());
    if (!latch.await(200, MILLISECONDS)) {
      fail("Task never executed");
    }
  }

  @Test
  @Description("Tests that a task submitted in excess of 'maxConcurrentTasks' waits until another task finishes before executing.")
  public void throttledTask() throws InterruptedException {
    final ScheduledExecutorService scheduler = createExecutor(THROTTLE_SIZE);
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

  protected ScheduledExecutorService createExecutor(int throttleSize) {
    return new ThrottledScheduler(BaseDefaultSchedulerTestCase.class.getSimpleName(), sharedExecutor, 1, sharedScheduledExecutor,
                                  sharedQuartzScheduler, CUSTOM, throttleSize, () -> 5000L, EMPTY_SHUTDOWN_CALLBACK);
  }

}
