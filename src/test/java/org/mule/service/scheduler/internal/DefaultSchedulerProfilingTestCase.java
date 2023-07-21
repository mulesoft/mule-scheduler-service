/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal;

import static java.util.Arrays.asList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DefaultSchedulerProfilingTestCase extends BaseDefaultSchedulerTestCase {

  private DefaultScheduler scheduler;

  @Override
  @Before
  public void before() throws Exception {
    super.before();
    scheduler = (DefaultScheduler) createExecutor();
  }

  @Parameterized.Parameters(name = "Profiling feature: {0}")
  public static List<Object[]> parameters() {
    return asList(
                  new Object[] {Boolean.FALSE},
                  new Object[] {Boolean.TRUE});
  }

  public DefaultSchedulerProfilingTestCase(Boolean isProfilingServiceEnabled) {
    this.isProfilingServiceEnabled = isProfilingServiceEnabled;
  }

  @Override
  @After
  public void after() throws Exception {
    scheduler.stop();
    scheduler = null;
    super.after();
  }

  @Test
  public void defaultSchedulerSubmitProfiling() throws InterruptedException {
    CountDownLatch remainingTasks = new CountDownLatch(2);
    scheduler.submit(remainingTasks::countDown);
    // TASK_EXECUTED profiling event cannot be verified without a new submit.
    // (it's triggered after the submitted task completion)
    scheduler.submit(remainingTasks::countDown);
    assertProfiling(remainingTasks);
  }

  @Test
  public void defaultSchedulerScheduleProfiling() throws InterruptedException {
    CountDownLatch remainingTasks = new CountDownLatch(2);
    scheduler.scheduleWithFixedDelay(() -> {
      remainingTasks.countDown();
      throw new RuntimeException("Subsequent executions should be cancelled by this error");
    }, 0, 365, TimeUnit.DAYS);
    // TASK_EXECUTED profiling event cannot be verified without a new submit.
    // (it's triggered after the submitted task completion)
    scheduler.scheduleWithFixedDelay(() -> {
      remainingTasks.countDown();
      throw new RuntimeException("Subsequent executions should be cancelled by this error");
    }, 0, 365, TimeUnit.DAYS);
    assertProfiling(remainingTasks);
  }

  private void assertProfiling(CountDownLatch remainingTasks) throws InterruptedException {
    remainingTasks.await();
    if (isProfilingServiceEnabled) {
      verify(schedulingTaskDataProducer, times(2)).triggerProfilingEvent(any());
      verify(executingTaskDataProducer, times(2)).triggerProfilingEvent(any());
      // Since the scheduler thread pool is of size one, at least one of the tasks has been fully executed.
      verify(executedTaskDataProducer, atLeastOnce()).triggerProfilingEvent(any());
    } else {
      verifyNoInteractions(schedulingTaskDataProducer);
      verifyNoInteractions(executingTaskDataProducer);
      verifyNoInteractions(executedTaskDataProducer);
    }
  }
}
