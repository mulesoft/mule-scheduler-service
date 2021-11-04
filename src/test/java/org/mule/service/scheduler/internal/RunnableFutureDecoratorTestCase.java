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
import static org.mule.tck.probe.PollingProber.DEFAULT_POLLING_INTERVAL;

import org.mule.runtime.api.profiling.tracing.ExecutionContext;
import org.mule.tck.probe.JUnitLambdaProbe;
import org.mule.tck.probe.PollingProber;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.MDC;

public class RunnableFutureDecoratorTestCase extends BaseDefaultSchedulerTestCase {

  private static final long GC_POLLING_TIMEOUT = 10000;

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
  public void threadLocalValuesNotLeaked() throws InterruptedException, ExecutionException {
    final ThreadLocalHolder tlHolder = new ThreadLocalHolder();

    Object valueToStore = new Object();
    PhantomReference ref = new PhantomReference<>(valueToStore, new ReferenceQueue<>());
    submitStore(tlHolder, valueToStore);

    valueToStore = null;

    new PollingProber(GC_POLLING_TIMEOUT, DEFAULT_POLLING_INTERVAL)
        .check(new JUnitLambdaProbe(() -> {
          System.gc();
          assertThat(ref.isEnqueued(), is(true));
          return true;
        }, "A hard reference is being mantained to the value stored in a thread local."));

  }

  private void submitStore(final ThreadLocalHolder tlHolder, Object valueToStore)
      throws InterruptedException, ExecutionException {
    scheduler.submit(() -> {
      tlHolder.set(valueToStore);
    }).get();
  }

  public static final class ThreadLocalHolder {

    private final ThreadLocal threadLocal = new ThreadLocal();

    public void set(Object value) {
      threadLocal.set(value);
    }
  }

  @Test
  public void mdcValuesNotSpreadAmongTasks() throws InterruptedException, ExecutionException {
    submitMdcPut();
    submitMdcGet();
  }

  // TODO: Test and assert profiling events emission

  // TODO: Modify RepeatableRunnableFutureDecorator accordingly

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

  private void submitMdcPut()
      throws InterruptedException, ExecutionException {
    scheduler.submit(() -> {
      MDC.put("key", "value");
    }).get();
  }

  private void submitMdcGet()
      throws InterruptedException, ExecutionException {
    assertThat(scheduler.submit(() -> {
      return MDC.get("key");
    }).get(), is(nullValue()));
  }
}
