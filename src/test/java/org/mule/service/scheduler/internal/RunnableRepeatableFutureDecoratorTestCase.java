/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal;

import static java.lang.Thread.currentThread;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mule.runtime.api.profiling.tracing.ExecutionContext;

import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
  public void executionContextPropagation() {
    ExecutionContext currentExecutionContext = mock(ExecutionContext.class);
    when(profilingService.getTracingService().getCurrentExecutionContext()).thenReturn(currentExecutionContext);
    RunnableRepeatableFutureDecorator<?> runnableFutureDecorator = getRunnableRepeatableFutureDecorator(() -> {
    });
    runnableFutureDecorator.run();
    verify(profilingService.getTracingService()).setCurrentExecutionContext(currentExecutionContext);
    verify(profilingService.getTracingService()).deleteCurrentExecutionContext();
  }

  @Test
  public void failedTaskExecutionContextPropagation() {
    ExecutionContext currentExecutionContext = mock(ExecutionContext.class);
    when(profilingService.getTracingService().getCurrentExecutionContext()).thenReturn(currentExecutionContext);
    RunnableRepeatableFutureDecorator<?> runnableFutureDecorator = getRunnableRepeatableFutureDecorator(() -> {
      throw new RuntimeException("This exception should not alter the execution context propagation");
    });
    try {
      runnableFutureDecorator.run();
    } catch (RuntimeException e) {
      verify(profilingService.getTracingService()).setCurrentExecutionContext(currentExecutionContext);
      verify(profilingService.getTracingService()).deleteCurrentExecutionContext();
    }
  }

  private RunnableRepeatableFutureDecorator<?> getRunnableRepeatableFutureDecorator(Runnable task) {
    return new RunnableRepeatableFutureDecorator<>(() -> new FutureTask<>(task, null),
                                                   task, objectRunnableRepeatableFutureDecorator -> {
                                                   }, RunnableRepeatableFutureDecoratorTestCase.class.getClassLoader(),
                                                   scheduler, "testTask", 1, profilingService);
  }

  private static class WrapUpException extends RuntimeException {

    private static final long serialVersionUID = 5170908600838156528L;

  }
}
