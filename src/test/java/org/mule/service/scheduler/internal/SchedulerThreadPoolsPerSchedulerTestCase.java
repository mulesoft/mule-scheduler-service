/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerPoolStrategy;
import org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig;
import org.mule.service.scheduler.internal.threads.SchedulerThreadPools;
import org.mule.tck.probe.JUnitLambdaProbe;
import org.mule.tck.probe.PollingProber;
import org.slf4j.MDC;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mule.runtime.api.scheduler.SchedulerConfig.config;
import static org.mule.tck.probe.PollingProber.DEFAULT_POLLING_INTERVAL;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;

@Feature(SCHEDULER_SERVICE)
@RunWith(Parameterized.class)
public class SchedulerThreadPoolsPerSchedulerTestCase extends AbstractSchedulerThreadPoolsTestCase {

  @Parameterized.Parameters(name = "{0}, {2}")
  public static Iterable<Object[]> data() {
    List<SchedulerPoolStrategy> strategies = strategies();

    Map<String, Function<SchedulerThreadPools, Scheduler>> schedulerSuppliers =
        of("Custom",
           service -> service.createCustomScheduler(config().withMaxConcurrentTasks(1).withDirectRunCpuLightWhenTargetBusy(true),
                                                    CORES, () -> 1000L),
           "CPULight", service -> service.createCpuLightScheduler(config(), CORES, () -> 1000L),
           "CPUIntensive", service -> service.createCpuIntensiveScheduler(config(), CORES, () -> 1000L),
           "IO", service -> service.createIoScheduler(config(), CORES, () -> 1000L));

    // Combines the strategies with the schedulers.
    List<Object[]> data = new ArrayList<>(strategies.size() * schedulerSuppliers.size());
    for (SchedulerPoolStrategy strategy : strategies) {
      for (Entry<String, Function<SchedulerThreadPools, Scheduler>> schedulerSupplierEntry : schedulerSuppliers.entrySet()) {
        data.add(new Object[] {strategy, schedulerSupplierEntry.getValue(), schedulerSupplierEntry.getKey()});
      }
    }

    return data;
  }

  protected Function<SchedulerThreadPools, Scheduler> schedulerProvider;
  protected SchedulerPoolStrategy strategy;

  public SchedulerThreadPoolsPerSchedulerTestCase(SchedulerPoolStrategy strategy,
                                                  Function<SchedulerThreadPools, Scheduler> schedulerProvider,
                                                  @SuppressWarnings("unused") String schedulerType) {
    super(strategy);
    this.schedulerProvider = schedulerProvider;
  }

  @Override
  protected ContainerThreadPoolsConfig buildThreadPoolsConfig() throws MuleException {
    // We want to have all pools with only one worker, so we get always the same thread for the same pool.
    // This simplifies our assertions a lot.
    ContainerThreadPoolsConfig threadPoolsConfig = spy(super.buildThreadPoolsConfig());
    when(threadPoolsConfig.getUberCorePoolSize()).thenReturn(OptionalInt.of(1));
    when(threadPoolsConfig.getUberMaxPoolSize()).thenReturn(OptionalInt.of(1));
    when(threadPoolsConfig.getCpuLightPoolSize()).thenReturn(OptionalInt.of(1));
    when(threadPoolsConfig.getCpuIntensivePoolSize()).thenReturn(OptionalInt.of(1));
    when(threadPoolsConfig.getIoCorePoolSize()).thenReturn(OptionalInt.of(1));
    when(threadPoolsConfig.getIoMaxPoolSize()).thenReturn(OptionalInt.of(1));
    return threadPoolsConfig;
  }

  @Test
  @Description("Checks that objects referenced by thread locals are reclaimable after each task finishes")
  public void threadLocalValuesNotLeaked() throws InterruptedException, ExecutionException {
    Scheduler scheduler = schedulerProvider.apply(service);
    ThreadLocal<Object> threadLocal = new ThreadLocal<>();

    Object valueToStore = new Object();
    PhantomReference<Object> ref = new PhantomReference<>(valueToStore, new ReferenceQueue<>());
    submitStore(scheduler, threadLocal, valueToStore);

    // noinspection UnusedAssignment
    valueToStore = null;

    new PollingProber(GC_POLLING_TIMEOUT, DEFAULT_POLLING_INTERVAL)
        .check(new JUnitLambdaProbe(() -> {
          System.gc();
          assertThat(ref.isEnqueued(), is(true));
          return true;
        }, "A hard reference is being maintained to the value stored in a thread local."));

  }

  @Test
  @Description("Checks that MDC values do not survive from task to task.")
  public void mdcValuesNotSpreadAmongTasks() throws InterruptedException, ExecutionException {
    Scheduler scheduler = schedulerProvider.apply(service);
    scheduler.submit(() -> MDC.put("key", "value")).get();
    submitMdcPut(scheduler);
    submitMdcGet(scheduler);
  }

  private void submitMdcPut(Scheduler scheduler)
      throws InterruptedException, ExecutionException {
    scheduler.submit(() -> MDC.put("key", "value")).get();
  }

  private void submitMdcGet(Scheduler scheduler)
      throws InterruptedException, ExecutionException {
    assertThat(scheduler.submit(() -> MDC.get("key")).get(), is(Matchers.nullValue()));
  }

  private void submitStore(Scheduler scheduler, final ThreadLocal<Object> threadLocal, final Object valueToStore)
      throws InterruptedException, ExecutionException {
    scheduler.submit(() -> threadLocal.set(valueToStore)).get();
  }
}
