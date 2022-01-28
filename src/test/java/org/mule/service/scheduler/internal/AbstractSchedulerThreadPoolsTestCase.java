/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import io.qameta.allure.Feature;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.exception.MuleRuntimeException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerPoolStrategy;
import org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig;
import org.mule.service.scheduler.internal.threads.SchedulerThreadPools;
import org.mule.tck.junit4.AbstractMuleTestCase;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Runtime.getRuntime;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.DEDICATED;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.UBER;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.loadThreadPoolsConfig;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;

@Feature(SCHEDULER_SERVICE)
@RunWith(Parameterized.class)
public abstract class AbstractSchedulerThreadPoolsTestCase extends AbstractMuleTestCase {

  protected static final int CORES = getRuntime().availableProcessors();
  protected static final long GC_POLLING_TIMEOUT = 10000;

  @Parameterized.Parameters(name = "{0}")
  public static List<SchedulerPoolStrategy> strategies() {
    return asList(DEDICATED, UBER);
  }

  protected SchedulerPoolStrategy strategy;
  protected ContainerThreadPoolsConfig threadPoolsConfig;
  protected SchedulerThreadPools service;

  protected long prestarCallbackSleepTime = 0L;

  public AbstractSchedulerThreadPoolsTestCase(SchedulerPoolStrategy strategy) {
    this.strategy = strategy;
  }

  @Before
  public void before() throws MuleException {
    prestarCallbackSleepTime = 0L;
    threadPoolsConfig = buildThreadPoolsConfig();

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

  protected ContainerThreadPoolsConfig buildThreadPoolsConfig() throws MuleException {
    ContainerThreadPoolsConfig threadPoolsConfig = loadThreadPoolsConfig();
    threadPoolsConfig.setSchedulerPoolStrategy(strategy, true);
    return threadPoolsConfig;
  }
}
