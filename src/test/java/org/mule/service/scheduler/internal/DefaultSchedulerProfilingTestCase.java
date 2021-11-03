/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static org.mule.runtime.api.profiling.type.RuntimeProfilingEventTypes.SCHEDULING_TASK_EXECUTION;
import static org.mule.runtime.api.profiling.type.RuntimeProfilingEventTypes.STARTING_TASK_EXECUTION;
import static org.mule.runtime.api.profiling.type.RuntimeProfilingEventTypes.TASK_EXECUTED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mule.runtime.core.internal.profiling.producer.TaskSchedulingProfilingDataProducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DefaultSchedulerProfilingTestCase extends BaseDefaultSchedulerTestCase {

  private DefaultScheduler scheduler;

  TaskSchedulingProfilingDataProducer schedulingTaskDataProducer = mock(TaskSchedulingProfilingDataProducer.class);
  TaskSchedulingProfilingDataProducer executingTaskDataProducer = mock(TaskSchedulingProfilingDataProducer.class);
  TaskSchedulingProfilingDataProducer executedTaskDataProducer = mock(TaskSchedulingProfilingDataProducer.class);

  @Override
  @Before
  public void before() throws Exception {
    super.before();
    when(profilingService.getProfilingDataProducer(SCHEDULING_TASK_EXECUTION)).thenReturn(schedulingTaskDataProducer);
    when(profilingService.getProfilingDataProducer(STARTING_TASK_EXECUTION)).thenReturn(executingTaskDataProducer);
    when(profilingService.getProfilingDataProducer(STARTING_TASK_EXECUTION)).thenReturn(executingTaskDataProducer);
    when(profilingService.getProfilingDataProducer(TASK_EXECUTED)).thenReturn(executedTaskDataProducer);
    scheduler = (DefaultScheduler) createExecutor();
  }

  @Override
  @After
  public void after() throws Exception {
    scheduler.stop();
    scheduler = null;
    super.after();
  }

  @Test
  public void defaultSchedulerSubmitProfiling() throws ExecutionException, InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);
    scheduler.submit(latch::countDown);
    // TASK_EXECUTED profiling event cannot be verified without a new submit.
    // (it's triggered after the submitted task completion)
    scheduler.submit(latch::countDown).get();
    verify(schedulingTaskDataProducer, times(2)).triggerProfilingEvent(any());
    verify(executingTaskDataProducer, times(2)).triggerProfilingEvent(any());
    // Since the scheduler thread pool is of size one, at least one of the tasks has been fully executed.
    verify(executedTaskDataProducer, atLeastOnce()).triggerProfilingEvent(any());
  }

  @Test
  public void defaultSchedulerScheduleProfiling() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(2);
    scheduler.scheduleWithFixedDelay(() -> {
      latch.countDown();
      throw new RuntimeException("Subsequent executions should be cancelled by this error");
    }, 0, 365, TimeUnit.DAYS);
    // TASK_EXECUTED profiling event cannot be verified without a new submit.
    // (it's triggered after the submitted task completion)
    scheduler.scheduleWithFixedDelay(() -> {
      latch.countDown();
      throw new RuntimeException("Subsequent executions should be cancelled by this error");
    }, 0, 365, TimeUnit.DAYS);
    latch.await();
    verify(schedulingTaskDataProducer, times(2)).triggerProfilingEvent(any());
    verify(executingTaskDataProducer, times(2)).triggerProfilingEvent(any());
    // Since the scheduler thread pool is of size one, at least one of the tasks has been fully executed.
    verify(executedTaskDataProducer, atLeastOnce()).triggerProfilingEvent(any());
  }
}
