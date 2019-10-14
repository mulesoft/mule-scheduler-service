/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.lang.Integer.toHexString;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.mule.runtime.api.scheduler.SchedulerConfig.config;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.DEDICATED;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.loadThreadPoolsConfig;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerConfig;
import org.mule.runtime.api.scheduler.SchedulerService;
import org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig;
import org.mule.service.scheduler.internal.threads.SchedulerThreadPools;
import org.mule.tck.junit4.AbstractMuleTestCase;
import org.mule.tck.probe.JUnitLambdaProbe;
import org.mule.tck.probe.PollingProber;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.qameta.allure.Feature;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * The names of the threads managed by the {@link SchedulerService} consist of some parts to aid in the monitoring and
 * troubleshooting of the Mule runtime and its deployed artifacts.
 * <p>
 * When a thread is idle, a thread name looks like:
 *
 * <pre>
 * {@code
 * [owner].type.index
 * }
 * </pre>
 *
 * When a thread is running a task, the name of the thread is changed to:
 *
 * <pre>
 * {@code
 * [owner].type.index: scheduler
 * }
 * </pre>
 *
 * Each part of the thread name has its meaning and provides meaningful information:
 * <ul>
 * <li>{@code owner}: The name of the artifact that owns the {@link Scheduler} that manages this thread's pool.</li>
 * <li>{@code type}: The {@link org.mule.service.scheduler.ThreadType} of this thread.</li>
 * <li>{@code index}: The id of this thread in its pool.</li>
 * <li>{@code scheduler}: The name of the {@link Scheduler} as provided though {@link SchedulerConfig#withName(String)} (for
 * instance, a processing strategy sets the name of the flow). If not provided, then the location in the code where the
 * {@link Scheduler} was created is shown. In either case, a hash is appended to differentiate different {@link Scheduler}s that
 * may have the same name.</li>
 * </ul>
 */
@Feature(SCHEDULER_SERVICE)
@RunWith(Parameterized.class)
public class DedicatedThreadNameTestCase extends AbstractMuleTestCase {

  private ContainerThreadPoolsConfig threadPoolsConfig;
  private SchedulerThreadPools service;

  private Function<SchedulerThreadPools, Scheduler> schedulerFactory;
  private Scheduler scheduler;
  private AtomicReference<Optional<AssertionError>> failureRef;
  private Function<Scheduler, Matcher<String>> prefixMatcher;

  @Parameters
  public static Collection<Object[]> data() {
    return asList(new Object[][] {
        {(Function<SchedulerThreadPools, Scheduler>) service -> service
            .createCpuLightScheduler(config().withName(DedicatedThreadNameTestCase.class.getSimpleName()), 1, () -> 1000L),
            (Function<Scheduler, Matcher<String>>) scheduler -> allOf(startsWith("[MuleRuntime].cpuLight"),
                                                                      endsWith(DedicatedThreadNameTestCase.class.getSimpleName()
                                                                          + " @"
                                                                          + toHexString(scheduler.hashCode())))},
        {(Function<SchedulerThreadPools, Scheduler>) service -> service.createCustomScheduler(config().withPrefix("owner")
            .withMaxConcurrentTasks(1).withName(DedicatedThreadNameTestCase.class.getSimpleName()), 1, () -> 1000L),
            (Function<Scheduler, Matcher<String>>) scheduler -> allOf(startsWith("[owner]."
                + DedicatedThreadNameTestCase.class.getSimpleName()))},
        {(Function<SchedulerThreadPools, Scheduler>) service -> service.createCustomScheduler(config().withMaxConcurrentTasks(1)
            .withName(DedicatedThreadNameTestCase.class.getSimpleName()), 1, () -> 1000L),
            (Function<Scheduler, Matcher<String>>) scheduler -> allOf(startsWith(DedicatedThreadNameTestCase.class
                .getSimpleName()))}
    });
  }

  public DedicatedThreadNameTestCase(Function<SchedulerThreadPools, Scheduler> schedulerFactory,
                                     Function<Scheduler, Matcher<String>> prefixMatcher) {
    this.schedulerFactory = schedulerFactory;
    this.prefixMatcher = prefixMatcher;
  }

  @Before
  public void before() throws MuleException {
    threadPoolsConfig = loadThreadPoolsConfig();
    threadPoolsConfig.setSchedulerPoolStrategy(DEDICATED, true);
    service = SchedulerThreadPools.builder(DedicatedThreadNameTestCase.class.getName(), threadPoolsConfig).build();
    service.start();

    scheduler = schedulerFactory.apply(service);

    failureRef = new AtomicReference<>(null);
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
  public void threadNameForOneShotTask() throws InterruptedException, ExecutionException, TimeoutException {
    scheduler.submit(new ThreadNameAssertingRunnable(failureRef, prefixMatcher.apply(scheduler)));

    pollingCheck();
  }

  @Test
  public void threadNameForRepeatableTask() throws InterruptedException, ExecutionException, TimeoutException {
    scheduler.scheduleAtFixedRate(new ThreadNameAssertingRunnable(failureRef, prefixMatcher.apply(scheduler)),
                                  0, 1, SECONDS);

    pollingCheck();
  }

  @Test
  public void threadNameForCronTask() throws InterruptedException, ExecutionException, TimeoutException {
    scheduler.scheduleWithCronExpression(new ThreadNameAssertingRunnable(failureRef, prefixMatcher.apply(scheduler)),
                                         "* * * * * ?");

    pollingCheck();
  }

  private void pollingCheck() {
    new PollingProber().check(new JUnitLambdaProbe(() -> {
      failureRef.get().ifPresent(f -> {
        throw f;
      });
      return true;
    }));
  }

  private class ThreadNameAssertingRunnable implements Runnable {

    private AtomicReference<Optional<AssertionError>> failureRef;
    private Matcher<String> threadNameMatcher;

    public ThreadNameAssertingRunnable(AtomicReference<Optional<AssertionError>> failureRef, Matcher<String> threadNameMatcher) {
      this.failureRef = failureRef;
      this.threadNameMatcher = threadNameMatcher;
    }

    @Override
    public void run() {
      try {
        assertThat(currentThread().getName(), threadNameMatcher);
        failureRef.set(empty());
      } catch (AssertionError e) {
        failureRef.set(of(e));
      }
    }
  }

}
