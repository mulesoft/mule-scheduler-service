/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * Provides test parameterization to test consistency in behaviur between Mule schedulers and java ExecutorServices.
 */
@RunWith(Parameterized.class)
public abstract class AbstractMuleVsJavaExecutorTestCase extends BaseDefaultSchedulerTestCase {

  private Function<AbstractMuleVsJavaExecutorTestCase, ScheduledExecutorService> executorFactory;

  protected ScheduledExecutorService executor;

  public AbstractMuleVsJavaExecutorTestCase(Function<AbstractMuleVsJavaExecutorTestCase, ScheduledExecutorService> executorFactory) {
    this.executorFactory = executorFactory;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return asList(new Object[][] {
        // Use a default ScheduledExecutorService to compare behavior
        {(Function<AbstractMuleVsJavaExecutorTestCase, ScheduledExecutorService>) test -> test
            .useSharedScheduledExecutor()},
        {(Function<AbstractMuleVsJavaExecutorTestCase, ScheduledExecutorService>) test -> test
            .createScheduledSameThreadExecutor()}
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

  protected ScheduledExecutorService useSharedScheduledExecutor() {
    sharedScheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(true);
    sharedScheduledExecutor.setRemoveOnCancelPolicy(true);

    return sharedScheduledExecutor;
  }

  protected ScheduledExecutorService createScheduledSameThreadExecutor() {
    return super.createExecutor();
  }

}
