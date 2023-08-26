/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.lang.Thread.currentThread;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.UBER;
import static org.mule.service.scheduler.ThreadType.CPU_INTENSIVE;
import static org.mule.service.scheduler.ThreadType.CPU_LIGHT;
import static org.mule.service.scheduler.ThreadType.IO;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;

import org.mule.runtime.api.scheduler.SchedulerPoolsConfig;

import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import io.qameta.allure.Feature;
import org.junit.Test;

@Feature(SCHEDULER_SERVICE)
public class UberPoolSchedulerServiceTestCase extends SchedulerServiceContractTestCase {

  @Test
  public void allSchedulerTypesShareSamePool() throws Exception {
    Set<ThreadGroup> groups = newSetFromMap(new ConcurrentHashMap<>());
    CountDownLatch latch = new CountDownLatch(3);

    Runnable task = () -> {
      groups.add(currentThread().getThreadGroup());
      latch.countDown();
    };

    service.cpuLightScheduler().submit((task));
    service.cpuIntensiveScheduler().submit(task);
    service.ioScheduler().submit(task);

    assertThat(latch.await(5, SECONDS), is(true));
    assertThat(groups, hasSize(1));
    ThreadGroup group = groups.iterator().next();

    assertThat(group, is(not(sameInstance(currentThread().getThreadGroup()))));
    assertThat(group.getName(), containsString(".uber"));
  }

  @Test
  public void threadsNamedCorrectly() throws Exception {
    CountDownLatch latch = new CountDownLatch(3);
    Runnable task = latch::countDown;

    service.cpuLightScheduler().submit((task));
    service.cpuIntensiveScheduler().submit(task);
    service.ioScheduler().submit(task);

    assertThat(latch.await(5, SECONDS), is(true));
    AtomicInteger uberCount = new AtomicInteger(0);

    Thread.getAllStackTraces().keySet().forEach(thread -> {
      assertThat(thread.getName(), not(containsString(IO.name())));
      assertThat(thread.getName(), not(containsString(CPU_LIGHT.name())));
      assertThat(thread.getName(), not(containsString(CPU_INTENSIVE.name())));

      if (thread.getName().contains(".uber")) {
        uberCount.addAndGet(1);
      }
    });

    assertThat(uberCount.get(), is(greaterThanOrEqualTo(1)));
  }

  @Override
  protected void configure(SchedulerPoolsConfig config) {
    when(config.getSchedulerPoolStrategy()).thenReturn(UBER);
    when(config.getUberCorePoolSize()).thenReturn(OptionalInt.of(1));
    when(config.getUberMaxPoolSize()).thenReturn(OptionalInt.of(1));
    when(config.getUberQueueSize()).thenReturn(OptionalInt.of(0));
    when(config.getUberKeepAlive()).thenReturn(OptionalLong.of(30000L));
  }
}
