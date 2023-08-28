/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.defaultThreadFactory;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mule.runtime.api.profiling.type.RuntimeProfilingEventTypes.SCHEDULING_TASK_EXECUTION;
import static org.mule.runtime.api.profiling.type.RuntimeProfilingEventTypes.STARTING_TASK_EXECUTION;
import static org.mule.runtime.api.profiling.type.RuntimeProfilingEventTypes.TASK_EXECUTED;
import static org.mule.service.scheduler.ThreadType.CUSTOM;

import org.mule.runtime.api.profiling.ProfilingDataProducer;
import org.mule.runtime.api.profiling.ProfilingService;
import org.mule.runtime.api.profiling.tracing.TracingService;
import org.mule.runtime.api.profiling.type.context.TaskSchedulingProfilingEventContext;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.tck.junit4.AbstractMuleTestCase;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.quartz.impl.StdSchedulerFactory;

public class BaseDefaultSchedulerTestCase extends AbstractMuleTestCase {

  protected static final int DELTA_MILLIS = 50;
  protected static final int EXECUTOR_TIMEOUT_SECS = 1;

  protected static final Runnable EMPTY_RUNNABLE = () -> {
  };
  protected static final Consumer<ScheduledExecutorService> SUBMIT_EMPTY_CALLABLE = exec -> exec.submit(() -> 0);
  protected static final Consumer<ScheduledExecutorService> SUBMIT_EMPTY_RUNNABLE = exec -> exec.submit(EMPTY_RUNNABLE);
  protected static final Consumer<ScheduledExecutorService> SUBMIT_RESULT_RUNNABLE = exec -> exec.submit(EMPTY_RUNNABLE, 0);
  protected static final Consumer<ScheduledExecutorService> EXECUTE_EMPTY_RUNNABLE = exec -> exec.execute(EMPTY_RUNNABLE);

  protected static final Consumer<Scheduler> EMPTY_SHUTDOWN_CALLBACK = scheduler -> {
  };

  protected boolean isProfilingServiceEnabled = false;

  @Rule
  public ExpectedException expected = ExpectedException.none();

  protected BlockingQueue<Runnable> sharedExecutorQueue = new ArrayBlockingQueue<>(1);
  protected ExecutorService sharedExecutor;
  protected ScheduledThreadPoolExecutor sharedScheduledExecutor;
  protected org.quartz.Scheduler sharedQuartzScheduler;

  // Mule profiling mocks
  protected final ProfilingService profilingService = mock(ProfilingService.class, RETURNS_MOCKS);
  private final TracingService tracingService = mock(TracingService.class);
  protected final ProfilingDataProducer<TaskSchedulingProfilingEventContext, Object> schedulingTaskDataProducer =
      mock(ProfilingDataProducer.class);
  protected final ProfilingDataProducer<TaskSchedulingProfilingEventContext, Object> executingTaskDataProducer =
      mock(ProfilingDataProducer.class);
  protected final ProfilingDataProducer<TaskSchedulingProfilingEventContext, Object> executedTaskDataProducer =
      mock(ProfilingDataProducer.class);

  @Before
  public void before() throws Exception {
    initializeProfilingMocks();

    sharedExecutor =
        new ThreadPoolExecutor(1, 1, 0, SECONDS, sharedExecutorQueue, defaultThreadFactory());
    sharedScheduledExecutor = spy(new ScheduledThreadPoolExecutor(1, defaultThreadFactory()));
    sharedScheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    sharedScheduledExecutor.setRemoveOnCancelPolicy(true);

    StdSchedulerFactory schedulerFactory = new StdSchedulerFactory();
    schedulerFactory.initialize(defaultQuartzProperties());
    sharedQuartzScheduler = spy(schedulerFactory.getScheduler());
    sharedQuartzScheduler.start();
  }

  @After
  public void after() throws Exception {
    sharedScheduledExecutor.shutdownNow();
    sharedQuartzScheduler.shutdown(true);
    sharedExecutor.shutdownNow();

    sharedScheduledExecutor.awaitTermination(5, SECONDS);
    sharedExecutor.awaitTermination(5, SECONDS);
  }

  protected void assertTerminationIsNotDelayed(final ScheduledExecutorService executor) throws InterruptedException {
    long startTime = nanoTime();
    executor.shutdown();
    executor.awaitTermination(1, SECONDS);

    assertThat((double) NANOSECONDS.toMillis(nanoTime() - startTime), closeTo(0, DELTA_MILLIS));
  }

  protected ScheduledExecutorService createExecutor() {
    return new DefaultScheduler(BaseDefaultSchedulerTestCase.class.getSimpleName(), sharedExecutor, 1, sharedScheduledExecutor,
                                sharedQuartzScheduler, CUSTOM, () -> 5000L, EMPTY_SHUTDOWN_CALLBACK,
                                isProfilingServiceEnabled ? profilingService : null);
  }

  protected boolean awaitLatch(final CountDownLatch latch) {
    try {
      return latch.await(10 * EXECUTOR_TIMEOUT_SECS, SECONDS);
    } catch (InterruptedException e) {
      currentThread().interrupt();
      return false;
    }
  }

  private void initializeProfilingMocks() {
    when(profilingService.getProfilingDataProducer(SCHEDULING_TASK_EXECUTION)).thenReturn(schedulingTaskDataProducer);
    when(profilingService.getProfilingDataProducer(STARTING_TASK_EXECUTION)).thenReturn(executingTaskDataProducer);
    when(profilingService.getProfilingDataProducer(STARTING_TASK_EXECUTION)).thenReturn(executingTaskDataProducer);
    when(profilingService.getProfilingDataProducer(TASK_EXECUTED)).thenReturn(executedTaskDataProducer);
    when(profilingService.getTracingService()).thenReturn(tracingService);
  }

  private Properties defaultQuartzProperties() {
    Properties factoryProperties = new Properties();

    factoryProperties.setProperty("org.quartz.scheduler.instanceName", getClass().getSimpleName());
    factoryProperties.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
    factoryProperties.setProperty("org.quartz.threadPool.threadNamePrefix", getClass().getSimpleName() + "_qz");
    factoryProperties.setProperty("org.quartz.threadPool.threadCount", "1");
    return factoryProperties;
  }

}
