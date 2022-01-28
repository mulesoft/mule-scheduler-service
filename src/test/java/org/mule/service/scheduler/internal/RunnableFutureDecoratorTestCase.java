/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mule.runtime.api.profiling.tracing.ExecutionContext;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.MDC;

public class RunnableFutureDecoratorTestCase extends BaseDefaultSchedulerTestCase {

  private DefaultScheduler scheduler;

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
  public void executionContextPropagation() {
    ExecutionContext currentExecutionContext = mock(ExecutionContext.class);
    when(profilingService.getTracingService().getCurrentExecutionContext()).thenReturn(currentExecutionContext);
    RunnableFutureDecorator<?> runnableFutureDecorator = getRunnableFutureDecorator(() -> null);
    runnableFutureDecorator.run();
    verify(profilingService.getTracingService()).setCurrentExecutionContext(currentExecutionContext);
    verify(profilingService.getTracingService()).deleteCurrentExecutionContext();
  }

  @Test
  public void failedTaskExecutionContextPropagation() {
    ExecutionContext currentExecutionContext = mock(ExecutionContext.class);
    when(profilingService.getTracingService().getCurrentExecutionContext()).thenReturn(currentExecutionContext);
    RunnableFutureDecorator<?> runnableFutureDecorator = getRunnableFutureDecorator(() -> {
      throw new RuntimeException("This exception should not alter the execution context propagation");
    });
    try {
      runnableFutureDecorator.run();
    } catch (RuntimeException e) {
      verify(profilingService.getTracingService()).setCurrentExecutionContext(currentExecutionContext);
      verify(profilingService.getTracingService()).deleteCurrentExecutionContext();
    }
  }

  private <T> RunnableFutureDecorator<T> getRunnableFutureDecorator(Callable<T> task) {
    return new RunnableFutureDecorator<T>(new FutureTask<T>(task), this.getClass().getClassLoader(), scheduler, "testTask", 1,
                                          profilingService);
  }
}
