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
import static org.mule.tck.probe.PollingProber.DEFAULT_POLLING_INTERVAL;

import org.mule.tck.probe.JUnitLambdaProbe;
import org.mule.tck.probe.PollingProber;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ExecutionException;

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
