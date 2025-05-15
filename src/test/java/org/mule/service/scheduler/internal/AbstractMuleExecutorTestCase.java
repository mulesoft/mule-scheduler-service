/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Provides test parameterization to test Mule schedulers
 */
@RunWith(Parameterized.class)
public abstract class AbstractMuleExecutorTestCase extends BaseDefaultSchedulerTestCase {

  private Function<AbstractMuleExecutorTestCase, ScheduledExecutorService> executorFactory;

  protected ScheduledExecutorService executor;

  public AbstractMuleExecutorTestCase(Function<AbstractMuleExecutorTestCase, ScheduledExecutorService> executorFactory,
                                      String param) {
    this.executorFactory = executorFactory;
  }

  @Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return asList(new Object[][] {
        {(Function<AbstractMuleExecutorTestCase, ScheduledExecutorService>) test -> test
            .createScheduledSameThreadExecutor(), "mule,syncQueue"},
        {(Function<AbstractMuleExecutorTestCase, ScheduledExecutorService>) test -> test
            .createScheduledSameThreadExecutor(), "mule,queue(1)"}
    });
  }

  @Override
  public void before() throws Exception {
    super.before();
    executor = createExecutor();
  }

  @Override
  public void after() throws Exception {
    executor.shutdownNow();
    executor.awaitTermination(5, SECONDS);
    super.after();
  }

  @Override
  protected ScheduledExecutorService createExecutor() {
    return executorFactory.apply(this);
  }

  protected ScheduledExecutorService createScheduledSameThreadExecutor() {
    return super.createExecutor();
  }

}
