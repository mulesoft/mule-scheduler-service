/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mule.runtime.api.scheduler.SchedulerConfig.config;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.loadThreadPoolsConfig;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SchedulerServiceStory.SHUTDOWN;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.exception.MuleRuntimeException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.util.concurrent.Latch;
import org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig;
import org.mule.service.scheduler.internal.threads.SchedulerThreadPools;
import org.mule.tck.junit4.AbstractMuleTestCase;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@Feature(SCHEDULER_SERVICE)
@Story(SHUTDOWN)
public class CustomSchedulerShutdownTestCase extends AbstractMuleTestCase {

  private ContainerThreadPoolsConfig threadPoolsConfig;
  private SchedulerThreadPools service;

  private final long prestarCallbackSleepTime = 0L;

  @Before
  public void before() throws MuleException {
    threadPoolsConfig = loadThreadPoolsConfig();
    service = SchedulerThreadPools.builder(SchedulerThreadPoolsTestCase.class.getName(), threadPoolsConfig)
        .setPreStartCallback(executor -> {
          try {
            sleep(prestarCallbackSleepTime);
          } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new MuleRuntimeException(e);
          }
        })
        .build();

    service.start();
  }

  @After
  public void after() throws MuleException, InterruptedException {
    if (service == null) {
      return;
    }
    for (Scheduler scheduler : new ArrayList<>(service.getSchedulers())) {
      scheduler.stop();
    }
    service.stop();
  }

  @Test
  public void shutdownDoesntDestroyThreadGroup() throws InterruptedException, ExecutionException {
    final Scheduler customScheduler = service.createCustomScheduler(config().withMaxConcurrentTasks(2), 2, () -> 10L);

    Latch latch = new Latch();
    AtomicReference<Thread> executedThread = new AtomicReference<>();
    final Future<Boolean> interrupted = customScheduler.submit(() -> {
      executedThread.set(currentThread());
      try {
        latch.await();
        return false;
      } catch (InterruptedException e) {
        currentThread().interrupt();
        return true;
      }
    });

    customScheduler.shutdown();

    Thread.sleep(5000);
    latch.release();

    assertThat(interrupted.get(), is(false));
  }

  @Test
  public void shutdownNowDestroysThreadGroup() throws InterruptedException, ExecutionException {
    final Scheduler customScheduler = service.createCustomScheduler(config().withMaxConcurrentTasks(2), 2, () -> 10L);

    Latch latch = new Latch();
    AtomicReference<Thread> executedThread = new AtomicReference<>();
    final Future<Boolean> interrupted = customScheduler.submit(() -> {
      executedThread.set(currentThread());
      try {
        latch.await();
        return false;
      } catch (InterruptedException e) {
        currentThread().interrupt();
        return true;
      }
    });

    customScheduler.shutdownNow();

    assertThat(interrupted.isCancelled(), is(true));
  }

}
