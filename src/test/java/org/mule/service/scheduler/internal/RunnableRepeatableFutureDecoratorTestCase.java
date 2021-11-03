/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.lang.Thread.currentThread;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.runtime.api.profiling.tracing.TracingContext;
import org.mule.runtime.api.profiling.tracing.TracingService;

public class RunnableRepeatableFutureDecoratorTestCase extends BaseDefaultSchedulerTestCase {

  private DefaultScheduler scheduler;

  private RunnableRepeatableFutureDecorator<Object> taskDecorator;

  @Override
  @Before
  public void before() throws Exception {
    super.before();
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
  public void exceptionInWrapUpCallbackCompletesWrapUp() {
    final ClassLoader taskClassloader = mock(ClassLoader.class);

    Runnable command = () -> {
    };
    taskDecorator =
        new RunnableRepeatableFutureDecorator<>(() -> new FutureTask<>(command, null), command, d -> {
          throw new WrapUpException();
        }, taskClassloader, scheduler, "testTask", -1, null);

    taskDecorator.run();

    assertThat(taskDecorator.isStarted(), is(false));
    assertThat(currentThread().getContextClassLoader(), not(taskClassloader));
  }

  @Test
  public void repeatableSecondRunBeforeFirstWrapUp() {
    final AtomicInteger runCount = new AtomicInteger(0);

    Runnable command = () -> {
      runCount.incrementAndGet();
    };
    taskDecorator =
        new RunnableRepeatableFutureDecorator<>(() -> new FutureTask<>(command, null), command, d -> {
          if (runCount.get() < 2) {
            taskDecorator.run();
          }
        }, RunnableRepeatableFutureDecoratorTestCase.class.getClassLoader(), scheduler, "testTask", -1, null);

    taskDecorator.run();

    assertThat(taskDecorator.isStarted(), is(false));
    assertThat(runCount.get(), is(2));
  }

  @Test
  public void tracingContextPropagation() throws ExecutionException, InterruptedException {
    TracingService tracingService = mock(TracingService.class);
    TracingContext currentTracingContext = mock(TracingContext.class);
    when(profilingService.getTracingService()).thenReturn(tracingService);
    when(tracingService.getCurrentTracingContext()).thenReturn(currentTracingContext);
    scheduler.submit(() -> {
    }).get();
    verify(profilingService.getTracingService()).setCurrentTracingContext(currentTracingContext);
    verify(profilingService.getTracingService()).deleteCurrentTracingContext();
  }

  private static class WrapUpException extends RuntimeException {

    private static final long serialVersionUID = 5170908600838156528L;

  }
}
