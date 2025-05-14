/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SchedulerServiceStory.TASK_SCHEDULING;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import io.qameta.allure.Issue;
import io.qameta.allure.Story;
import org.junit.Test;

@Feature(SCHEDULER_SERVICE)
@Story(TASK_SCHEDULING)
public class MuleDefaultSchedulerTestCase extends AbstractMuleExecutorTestCase {

  public MuleDefaultSchedulerTestCase(Function<AbstractMuleExecutorTestCase, ScheduledExecutorService> executorFactory,
                                      String param) {
    super(executorFactory, param);
  }

  @Test
  @Issue("W-18347237")
  @Description("Tests that a task with zero delay is executed immediately.")
  public void scheduleWithZeroDelay() throws InterruptedException, ExecutionException, TimeoutException {
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch taskLatch = new CountDownLatch(1);
    final long startTime = nanoTime();

    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      latch.countDown();
      awaitLatch(taskLatch);
    }, 0, NANOSECONDS);

    assertThat(latch.await(EXECUTOR_TIMEOUT_SECS, SECONDS), is(true));
    assertThat(nanoTime() - startTime, lessThan(SECONDS.toNanos(1)));
    taskLatch.countDown();
    assertThat(scheduled.get(), nullValue());
    assertThat(scheduled.get(EXECUTOR_TIMEOUT_SECS, SECONDS), nullValue());
    assertThat(scheduled.isDone(), is(true));
    assertThat(scheduled.getDelay(NANOSECONDS), is(0L));
  }

  @Test
  @Issue("W-18347237")
  @Description("Tests that a task with maximum delay is cancelled.")
  public void scheduleWithMaxDelay() {
    final CountDownLatch latch = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      latch.countDown();
    }, Long.MAX_VALUE, NANOSECONDS);

    assertThat(scheduled.isCancelled(), is(true));
    assertThat(scheduled.isDone(), is(true));
    assertThat(latch.getCount(), is(1L)); // task should not execute
  }

  @Test
  @Description("Tests that the compareTo method orders tasks by their delay correctly")
  public void compareToOrdersTasksByDelay() {
    // Create futures with different delays
    final ScheduledFuture<?> shortDelay = executor.schedule(() -> {
    }, 10, NANOSECONDS);
    final ScheduledFuture<?> longDelay = executor.schedule(() -> {
    }, 100, NANOSECONDS);

    // Zero delay should come before positive delay in ordering
    assertThat(shortDelay.compareTo(longDelay) < 0, is(true));
    assertThat(longDelay.compareTo(shortDelay) > 0, is(true));
  }
}
