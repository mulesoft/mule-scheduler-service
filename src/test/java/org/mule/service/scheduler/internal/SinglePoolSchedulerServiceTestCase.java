/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
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
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import io.qameta.allure.Feature;
import org.junit.Test;

@Feature(SCHEDULER_SERVICE)
public class SinglePoolSchedulerServiceTestCase extends SchedulerServiceContractTestCase {

  @Override
  protected SchedulerServiceAdapter createSchedulerService() {
    return new SinglePoolSchedulerService();
  }

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
    assertThat(groups.iterator().next(), is(not(sameInstance(currentThread().getThreadGroup()))));
  }
}
