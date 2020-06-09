/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.lang.Runtime.getRuntime;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.junit.rules.ExpectedException.none;
import static org.mockito.Mockito.mock;
import static org.mule.runtime.api.scheduler.SchedulerConfig.config;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.DEDICATED;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.UBER;
import static org.mule.runtime.core.api.util.ClassUtils.withContextClassLoader;
import static org.mule.runtime.core.api.util.IOUtils.toByteArray;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.loadThreadPoolsConfig;
import static org.mule.tck.probe.PollingProber.DEFAULT_POLLING_INTERVAL;
import static org.mule.tck.probe.PollingProber.probe;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.exception.MuleRuntimeException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerBusyException;
import org.mule.runtime.api.scheduler.SchedulerConfig;
import org.mule.runtime.api.scheduler.SchedulerPoolStrategy;
import org.mule.runtime.api.util.concurrent.Latch;
import org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig;
import org.mule.service.scheduler.internal.threads.SchedulerThreadPools;
import org.mule.service.scheduler.internal.util.Delegator;
import org.mule.tck.junit4.AbstractMuleTestCase;
import org.mule.tck.probe.JUnitLambdaProbe;
import org.mule.tck.probe.PollingProber;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import io.qameta.allure.Issue;

@Feature(SCHEDULER_SERVICE)
@RunWith(Parameterized.class)
public class SchedulerThreadPoolsTestCase extends AbstractMuleTestCase {

  private static final int CORES = getRuntime().availableProcessors();
  private static final long GC_POLLING_TIMEOUT = 10000;

  @Parameters(name = "{0}")
  public static List<SchedulerPoolStrategy> parameters() {
    return asList(DEDICATED, UBER);
  }

  @Parameter
  public SchedulerPoolStrategy strategy;

  @Rule
  public ExpectedException expected = none();

  private ContainerThreadPoolsConfig threadPoolsConfig;
  private SchedulerThreadPools service;

  private long prestarCallbackSleepTime = 0L;

  @Before
  public void before() throws MuleException {
    threadPoolsConfig = loadThreadPoolsConfig();
    threadPoolsConfig.setSchedulerPoolStrategy(strategy, true);

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
  @Description("Tests that the threads of the SchedulerService are correcly created and destroyed.")
  public void serviceStop() throws MuleException, InterruptedException {
    assertThat(collectThreadNames(), hasItem(startsWith("[MuleRuntime].")));

    service.stop();
    service = null;

    new PollingProber(500, 50).check(new JUnitLambdaProbe(() -> {
      assertThat(collectThreadNames(), not(hasItem(startsWith("[MuleRuntime]."))));
      return true;
    }));
  }

  @Test
  @Description("Tests that dispatching a task to a throttled scheduler already running its maximum tasks throws the appropriate exception.")
  public void executorRejects() throws MuleException, ExecutionException, InterruptedException {
    final Latch latch = new Latch();

    final Scheduler cpuLight =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);
    final Scheduler custom =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);

    // this will execute immediately
    custom.execute(() -> {
      awaitLatch(latch);
    });
    // this will be queued
    custom.execute(() -> {
      awaitLatch(latch);
    });

    expected.expect(ExecutionException.class);
    expected.expectCause(instanceOf(SchedulerBusyException.class));

    final Runnable task = () -> {
    };
    cpuLight.submit(() -> {
      try {
        custom.submit(task);
      } finally {
        assertThat(custom.shutdownNow(), not(hasItem(task)));
      }
    }).get();
  }

  @Test
  @Description("Tests that a dispatched task has inherited the context classloader.")
  public void classLoaderPropagates() throws Exception {
    final Scheduler scheduler = service.createCpuLightScheduler(config(), CORES, () -> 1000L);

    final ClassLoader contextClassLoader = mock(ClassLoader.class);
    currentThread().setContextClassLoader(contextClassLoader);

    final Future<?> submit = scheduler.submit(() -> {
      assertThat(currentThread().getContextClassLoader(), sameInstance(contextClassLoader));
    });

    submit.get(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);
  }

  @Test
  @Description("Tests that a scheduled task has inherited the context classloader.")
  public void classLoaderPropagatesScheduled() throws Exception {
    final Scheduler scheduler = service.createCpuLightScheduler(config(), CORES, () -> 1000L);

    final ClassLoader contextClassLoader = mock(ClassLoader.class);
    currentThread().setContextClassLoader(contextClassLoader);

    Latch latch = new Latch();
    ScheduledFuture<?> submit = null;
    try {
      submit = scheduler.scheduleWithFixedDelay(() -> {
        assertThat(currentThread().getContextClassLoader(), sameInstance(contextClassLoader));
        latch.countDown();
      }, 0, 60, SECONDS);

      latch.await(10, SECONDS);
      submit.get(10, SECONDS);
    } finally {
      if (submit != null) {
        submit.cancel(false);
      }
    }
  }

  @Test
  @Description("Tests that a cron-scheduled task has inherited the context classloader.")
  public void classLoaderPropagatesCron() throws Exception {
    final Scheduler scheduler = service.createCpuLightScheduler(config(), CORES, () -> 1000L);

    final ClassLoader contextClassLoader = mock(ClassLoader.class);
    currentThread().setContextClassLoader(contextClassLoader);

    Latch latch = new Latch();
    ScheduledFuture<?> submit = null;
    try {
      submit = scheduler.scheduleWithCronExpression(() -> {
        assertThat(currentThread().getContextClassLoader(), sameInstance(contextClassLoader));
        latch.countDown();
      }, "*/1 * * ? * *");

      latch.await(10, SECONDS);
      submit.get(10, SECONDS);
    } finally {
      if (submit != null) {
        submit.cancel(false);
      }
    }
  }

  @Test
  @Description("Tests that a custom scheduler doesn't hold a reference to the context classloader that was in the context when it was created.")
  public void customPoolThreadsDontReferenceCreatorClassLoader() throws Exception {
    ClassLoader testClassLoader = new ClassLoader(this.getClass().getClassLoader()) {};
    PhantomReference<ClassLoader> clRef = new PhantomReference<>(testClassLoader, new ReferenceQueue<>());

    scheduleToCustomWithClassLoader(testClassLoader);

    testClassLoader = null;

    assertNoClassLoaderReferenceHeld(clRef, GC_POLLING_TIMEOUT);
  }

  public void scheduleToCustomWithClassLoader(final ClassLoader testClassLoader) throws InterruptedException, ExecutionException {
    final AtomicReference<Scheduler> scheduler = new AtomicReference<>();
    withContextClassLoader(testClassLoader, () -> {
      scheduler.set(service.createCustomScheduler(config().withMaxConcurrentTasks(1), 1, () -> 1000L));

      try {
        scheduler.get().submit(() -> {
          assertThat(currentThread().getContextClassLoader(), is(testClassLoader));
        }).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });

    scheduler.get().submit(() -> {
      assertThat(currentThread().getContextClassLoader(), is(testClassLoader.getParent()));
    }).get();
  }

  @Test
  @Description("Tests that a scheduler Executor thread doesn't hold a reference to an artifact classloader through the `inheritedAccessControlContext` when executing.")
  public void threadsDontReferenceClassLoaderFromAccessControlContext() throws Exception {
    Scheduler scheduler = service.createCustomScheduler(config().withMaxConcurrentTasks(1), 1, () -> 1000L);

    ClassLoader delegatorClassLoader = createDelegatorClassLoader();
    PhantomReference<ClassLoader> clRef = new PhantomReference<>(delegatorClassLoader, new ReferenceQueue<>());

    @SuppressWarnings("unchecked")
    Consumer<Runnable> delegator = (Consumer<Runnable>) delegatorClassLoader.loadClass(Delegator.class.getName()).newInstance();
    delegator.accept(() -> scheduler.execute(() -> {
    }));

    delegator = null;
    delegatorClassLoader = null;

    assertNoClassLoaderReferenceHeld(clRef, GC_POLLING_TIMEOUT);
  }

  @Test
  @Description("Tests that a scheduler Executor thread doesn't hold a reference to an artifact classloader through the `inheritedAccessControlContext` when created.")
  public void threadsDontReferenceClassLoaderFromAccessControlContextWhenCreated() throws Exception {
    ClassLoader delegatorClassLoader = createDelegatorClassLoader();
    PhantomReference<ClassLoader> clRef = new PhantomReference<>(delegatorClassLoader, new ReferenceQueue<>());

    AtomicReference<Scheduler> schedulerRef = new AtomicReference<>();
    @SuppressWarnings("unchecked")
    Consumer<Runnable> delegator = (Consumer<Runnable>) delegatorClassLoader.loadClass(Delegator.class.getName()).newInstance();
    delegator.accept(() -> {
      schedulerRef.set(service.createCustomScheduler(config().withMaxConcurrentTasks(1), 1, () -> 1000L));
    });

    delegator = null;
    delegatorClassLoader = null;

    assertNoClassLoaderReferenceHeld(clRef, GC_POLLING_TIMEOUT);
  }

  @Test
  @Description("Tests that IO threads in excess of the core size don't hold a reference to an artifact classloader through the inheritedAccessControlContext.")
  public void elasticIoThreadsDontReferenceClassLoaderFromAccessControlContext() throws Exception {
    assumeDedicatedStrategy();

    assertThat(threadPoolsConfig.getIoKeepAlive().getAsLong(), greaterThan(GC_POLLING_TIMEOUT));

    Scheduler scheduler = service.createIoScheduler(config(), threadPoolsConfig.getIoCorePoolSize().getAsInt() + 1, () -> 1000L);

    ClassLoader delegatorClassLoader = createDelegatorClassLoader();
    PhantomReference<ClassLoader> clRef = new PhantomReference<>(delegatorClassLoader, new ReferenceQueue<>());

    @SuppressWarnings("unchecked")
    Consumer<Runnable> delegator = (Consumer<Runnable>) delegatorClassLoader.loadClass(Delegator.class.getName()).newInstance();
    for (int i = 0; i < threadPoolsConfig.getIoCorePoolSize().getAsInt() + 1; ++i) {
      delegator.accept(() -> scheduler.execute(() -> {
      }));
    }

    delegator = null;
    delegatorClassLoader = null;

    assertNoClassLoaderReferenceHeld(clRef, GC_POLLING_TIMEOUT);
  }

  @Test
  @Description("Tests that when using a the commonPool from ForkJoinPool, the TCCL of the first invocation is not leaked."
      + " This test essentially validates the workaround for https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8172726")
  public void forkJoinCommonPoolDoesNotLeakFirstClassLoaderUsed()
      throws InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException {
    commonPool().shutdownNow();
    Scheduler scheduler = service.createIoScheduler(config(), 1, () -> 1000L);

    AtomicReference<PhantomReference<ClassLoader>> clRefRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(CORES + 1);

    scheduler.execute(() -> {
      ClassLoader delegatorClassLoader = createDelegatorClassLoader();
      PhantomReference<ClassLoader> clRef = new PhantomReference<>(delegatorClassLoader, new ReferenceQueue<>());

      Thread.currentThread().setContextClassLoader(delegatorClassLoader);

      for (int i = 0; i < CORES; ++i) {
        commonPool().execute(() -> {
          // Nothing to do
          latch.countDown();
        });
      }

      clRefRef.set(clRef);
      delegatorClassLoader = null;
      latch.countDown();
    });

    latch.await(5, SECONDS);

    assertNoClassLoaderReferenceHeld(clRefRef.get(), GC_POLLING_TIMEOUT);
  }

  @Test
  @Issue("MULE-18471")
  @Description("Attempts to force a race condition between stopping a scheduler and periodic tasks being rescheduled.")
  public void avoidRaceConditionBetweenStopAndRescheduleFixedDelayCausingLeak()
      throws InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException {
    final Scheduler customScheduler = service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);

    ClassLoader delegatorClassLoader = createDelegatorClassLoader();
    PhantomReference<ClassLoader> clRef = new PhantomReference<>(delegatorClassLoader, new ReferenceQueue<>());

    @SuppressWarnings("unchecked")
    Consumer<Runnable> delegator = (Consumer<Runnable>) delegatorClassLoader.loadClass(Delegator.class.getName()).newInstance();

    final ExecutorService scheduleExecutor = newFixedThreadPool(CORES * 2);
    for (int i = 0; i < CORES * 24; ++i) {
      scheduleTaskReferencingDelegator(scheduleExecutor, customScheduler, delegator);
    }

    delegator = null;
    delegatorClassLoader = null;

    sleep(DEFAULT_POLLING_INTERVAL);
    customScheduler.stop();

    assertNoClassLoaderReferenceHeld(clRef, GC_POLLING_TIMEOUT);

    scheduleExecutor.shutdownNow();
  }

  private void scheduleTaskReferencingDelegator(Executor scheduleExecutor, final Scheduler customScheduler,
                                                Consumer<Runnable> delegator) {
    scheduleExecutor.execute(() -> customScheduler.scheduleWithFixedDelay(() -> delegator.accept(() -> {
    }), 0, 1, NANOSECONDS));
  }

  @Test
  @Issue("MULE-18471")
  @Description("Attempts to force a race condition between stopping a scheduler and a task being scheduled.")
  public void avoidRaceConditionBetweenStopAndScheduleFixedDelayCausingLeak()
      throws InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException {
    final Scheduler customScheduler = service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);

    ClassLoader delegatorClassLoader = createDelegatorClassLoader();
    PhantomReference<ClassLoader> clRef = new PhantomReference<>(delegatorClassLoader, new ReferenceQueue<>());

    @SuppressWarnings("unchecked")
    Consumer<Runnable> delegator = (Consumer<Runnable>) delegatorClassLoader.loadClass(Delegator.class.getName()).newInstance();

    final ExecutorService scheduleExecutor = newSingleThreadExecutor();
    scheduleTaskReferencingDelegatorPending(scheduleExecutor, customScheduler, delegator);

    delegator = null;
    delegatorClassLoader = null;

    sleep(DEFAULT_POLLING_INTERVAL);
    customScheduler.stop();

    assertNoClassLoaderReferenceHeld(clRef, GC_POLLING_TIMEOUT);

    scheduleExecutor.shutdownNow();
  }

  private void scheduleTaskReferencingDelegatorPending(Executor scheduleExecutor, final Scheduler customScheduler,
                                                       Consumer<Runnable> delegator) {
    scheduleExecutor.execute(() -> customScheduler.scheduleWithFixedDelay(() -> delegator.accept(() -> {
    }), 10000, 1, SECONDS));
  }

  private ClassLoader createDelegatorClassLoader() {
    // The inheritedAccessControlContext holds a reference to the classloaders of any class in the call stack that starts the
    // thread.
    // With this test, we ensure that the threads are started with only container/service code in the stack, and not from an
    // artifact classloader (represented here by this child classloader).
    ClassLoader testClassLoader = new ClassLoader(this.getClass().getClassLoader()) {

      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (Delegator.class.getName().equals(name)) {
          byte[] classBytes;
          try {
            classBytes =
                toByteArray(this.getClass().getResourceAsStream("/org/mule/service/scheduler/internal/util/Delegator.class"));
            return this.defineClass(null, classBytes, 0, classBytes.length);
          } catch (Exception e) {
            return super.loadClass(name);
          }
        } else {
          return super.loadClass(name);
        }
      }
    };
    return testClassLoader;
  }

  private void assertNoClassLoaderReferenceHeld(PhantomReference<ClassLoader> clRef, long timeoutMillis) {
    new PollingProber(timeoutMillis, DEFAULT_POLLING_INTERVAL)
        .check(new JUnitLambdaProbe(() -> {
          System.gc();
          assertThat(clRef.isEnqueued(), is(true));
          return true;
        }, "A hard reference is being mantained to the child ClassLoader."));
  }

  @Test
  public void threadGroupOfCustomSchedulerNotLeakedAfterShutdownNow()
      throws InterruptedException, ExecutionException, TimeoutException {
    Scheduler scheduler = service.createCustomScheduler(config().withMaxConcurrentTasks(1), 1, () -> 1000L);

    List<PhantomReference> references = recordReferences(scheduler);

    scheduler.shutdownNow();
    scheduler = null;

    assertNoThreadGroupReferenceHeld(references);
  }

  @Test
  public void threadGroupOfCustomSchedulerNotLeakedAfterStop() throws InterruptedException, ExecutionException, TimeoutException {
    Scheduler scheduler = service.createCustomScheduler(config().withMaxConcurrentTasks(1), 1, () -> 1000L);

    List<PhantomReference> references = recordReferences(scheduler);

    scheduler.stop();
    scheduler = null;

    assertNoThreadGroupReferenceHeld(references);
  }

  private List<PhantomReference> recordReferences(Scheduler scheduler)
      throws InterruptedException, ExecutionException, TimeoutException {
    List<PhantomReference> references = new ArrayList<>();

    scheduler.submit(() -> {
      references.add(new PhantomReference<>(currentThread(), new ReferenceQueue<>()));
      references.add(new PhantomReference<>(currentThread().getThreadGroup(), new ReferenceQueue<>()));
      return true;
    }).get(5, SECONDS);
    return references;
  }

  private void assertNoThreadGroupReferenceHeld(List<PhantomReference> references) {
    new PollingProber(GC_POLLING_TIMEOUT, DEFAULT_POLLING_INTERVAL)
        .check(new JUnitLambdaProbe(() -> {
          System.gc();
          references.forEach(ref -> assertThat(ref.toString(), ref.isEnqueued(), is(true)));
          return true;
        }, "A hard reference is being mantained to the scheduler threads/thread group."));
  }

  @Test
  public void customSchedulerPrestarted() throws Exception {
    prestarCallbackSleepTime = 1000L;

    // Need a CPU bound scheduler to force rejection exception instead of retry
    final Scheduler cpuBoundScheduler = service.createCpuIntensiveScheduler(config().withMaxConcurrentTasks(1), 1, () -> 1000L);

    cpuBoundScheduler.submit(() -> {
      final SchedulerConfig bigPoolConfig = config().withMaxConcurrentTasks(1);
      Scheduler scheduler;

      for (int i = 0; i < 10; ++i) {
        final CountDownLatch latch = new CountDownLatch(1);
        scheduler = service.createCustomScheduler(bigPoolConfig, 1, () -> 0L);

        try {
          // asserting that tasks can be submitted right after the scheduler is returned.
          scheduler.submit(() -> {
            latch.countDown();
          });
          try {
            latch.await(5, SECONDS);
          } catch (InterruptedException e) {
            currentThread().interrupt();
            return;
          }
        } finally {
          scheduler.stop();
        }
      }
    }).get();
  }

  @Test
  public void customSchedulerShutdownFromWithin() throws Exception {
    Scheduler scheduler = service.createCustomScheduler(config().withMaxConcurrentTasks(1), 1, () -> 1000L);
    AtomicReference<ThreadGroup> customThreadGroup = new AtomicReference<>();

    Future<?> stopSubmit = scheduler.submit(() -> {
      customThreadGroup.set(currentThread().getThreadGroup());
      scheduler.stop();
    });

    expected.expect(CancellationException.class);
    try {
      stopSubmit.get(10, SECONDS);
    } finally {
      new PollingProber().check(new JUnitLambdaProbe(() -> {
        assertThat("Shutdown", scheduler.isShutdown(), is(true));
        assertThat("Terminated", scheduler.isTerminated(), is(true));
        assertThat("Destroyed", customThreadGroup.get().isDestroyed(), is(true));
        return true;
      }));
    }
  }

  @Test
  public void customSchedulerShutdownFromWithinDelayed() throws Exception {
    Scheduler scheduler = service.createCustomScheduler(config().withMaxConcurrentTasks(2), 2, () -> 1000L);
    AtomicReference<ThreadGroup> customThreadGroup = new AtomicReference<>();
    AtomicBoolean cancelled = new AtomicBoolean(false);

    Future<?> hangSubmit = scheduler.submit(() -> {
      while (!cancelled.get()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          currentThread().interrupt();
        }
      }
    });
    Future<?> stopSubmit = scheduler.submit(() -> {
      customThreadGroup.set(currentThread().getThreadGroup());
      scheduler.stop();
    });

    expected.expect(CancellationException.class);
    try {
      stopSubmit.get(10, SECONDS);
    } finally {
      cancelled.set(true);
      new PollingProber().check(new JUnitLambdaProbe(() -> {
        assertThat("Shutdown", scheduler.isShutdown(), is(true));
        assertThat("Terminated", scheduler.isTerminated(), is(true));
        assertThat("Destroyed", customThreadGroup.get().isDestroyed(), is(true));
        return true;
      }));
    }
  }

  @Test
  public void onlyCustomMayConfigureWaitCpuLight() {
    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("Only custom schedulers may define 'waitAllowed' behaviour");
    service.createCpuLightScheduler(config().withWaitAllowed(true), CORES, () -> 1000L);
  }

  @Test
  public void onlyCustomMayConfigureWaitCpuIntensive() {
    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("Only custom schedulers may define 'waitAllowed' behaviour");
    service.createCpuIntensiveScheduler(config().withWaitAllowed(true), CORES, () -> 1000L);
  }

  @Test
  public void onlyCustomMayConfigureWaitIO() {
    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("Only custom schedulers may define 'waitAllowed' behaviour");
    service.createIoScheduler(config().withWaitAllowed(true), CORES, () -> 1000L);
  }

  @Test
  public void onlyCustomMayConfigureDirectRunCpuLightWhenTargetBusyCpuLight() {
    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("Only custom schedulers may define 'directRunCpuLightWhenTargetBusy' behaviour");
    service.createCpuLightScheduler(config().withDirectRunCpuLightWhenTargetBusy(true), CORES, () -> 1000L);
  }

  @Test
  public void onlyCustomMayConfigureDirectRunCpuLightWhenTargetBusyCpuIntensive() {
    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("Only custom schedulers may define 'directRunCpuLightWhenTargetBusy' behaviour");
    service.createCpuIntensiveScheduler(config().withDirectRunCpuLightWhenTargetBusy(true), CORES, () -> 1000L);
  }

  @Test
  public void onlyCustomMayConfigureDirectRunCpuLightWhenTargetBusyIO() {
    expected.expect(IllegalArgumentException.class);
    expected.expectMessage("Only custom schedulers may define 'directRunCpuLightWhenTargetBusy' behaviour");
    service.createIoScheduler(config().withDirectRunCpuLightWhenTargetBusy(true), CORES, () -> 1000L);
  }

  @Test
  @Description("Tests that tasks dispatched from a CPU Light thread to a busy Scheduler are rejected.")
  public void rejectionPolicyCpuLight() throws MuleException, InterruptedException, ExecutionException, TimeoutException {
    assumeDedicatedStrategy();

    Scheduler sourceScheduler = service.createCpuLightScheduler(config(), CORES, () -> 1000L);
    Scheduler targetScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);

    Latch latch = new Latch();

    Future<Object> submit = sourceScheduler.submit(threadsConsumer(targetScheduler, latch));

    expected.expect(ExecutionException.class);
    expected.expectCause(instanceOf(SchedulerBusyException.class));
    submit.get(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);
  }

  @Test
  @Description("Tests that tasks dispatched from a CPU Intensive thread to a busy Scheduler are rejected.")
  public void rejectionPolicyCpuIntensive() throws MuleException, InterruptedException, ExecutionException, TimeoutException {
    assumeDedicatedStrategy();

    Scheduler sourceScheduler = service.createCpuIntensiveScheduler(config(), CORES, () -> 1000L);
    Scheduler targetScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);

    Latch latch = new Latch();

    Future<Object> submit = sourceScheduler.submit(threadsConsumer(targetScheduler, latch));

    expected.expect(ExecutionException.class);
    expected.expectCause(instanceOf(SchedulerBusyException.class));
    submit.get(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);
  }

  @Test
  @Description("Tests that tasks dispatched from an IO thread to a busy Scheduler waits for execution.")
  public void rejectionPolicyIO() throws MuleException, InterruptedException, ExecutionException, TimeoutException {
    Scheduler sourceScheduler = service.createIoScheduler(config(), CORES, () -> 1000L);
    Scheduler targetScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);

    Latch latch = new Latch();

    Future<Object> submit = sourceScheduler.submit(threadsConsumer(targetScheduler, latch));

    try {
      submit.get(1, SECONDS);
      fail();
    } catch (TimeoutException te) {
    }

    latch.countDown();
    submit.get(5, SECONDS);
  }

  @Test
  @Description("Tests that when the IO pool is full, any task dispatched from IO to IO runs in the caller thread instead of being queued, which can cause a deadlock.")
  public void ioToFullIoDoesntWait() throws InterruptedException, ExecutionException {
    assumeDedicatedStrategy();

    Scheduler ioScheduler = service.createIoScheduler(config(), CORES, () -> 1000L);

    Latch outerLatch = new Latch();
    Latch innerLatch = new Latch();

    // Fill up the IO pool, leaving room for just one more task
    for (int i = 0; i < threadPoolsConfig.getIoMaxPoolSize().getAsInt() - 1; ++i) {
      consumeThread(ioScheduler, outerLatch);
    }

    AtomicReference<Thread> callerThread = new AtomicReference<>();
    AtomicReference<Thread> executingThread = new AtomicReference<>();

    // The outer task will use the remaining slot in the scheduler, causing it to be full when the inner is sent.
    Future<Boolean> submitted = ioScheduler.submit(() -> {
      callerThread.set(currentThread());

      ioScheduler.submit(() -> {
        executingThread.set(currentThread());
        innerLatch.countDown();
      });

      return awaitLatch(outerLatch);
    });

    assertThat(innerLatch.await(5, SECONDS), is(true));
    outerLatch.countDown();
    assertThat(submitted.get(), is(true));
    assertThat(executingThread.get(), is(callerThread.get()));
  }

  @Test
  @Description("Tests that when the IO pool is full, any task dispatched from a CUSTOM pool with WAIT rejection action to IO is queued.")
  public void customWaitToFullIoWaits() throws InterruptedException, ExecutionException, TimeoutException {
    assumeDedicatedStrategy();

    Scheduler customScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1).withWaitAllowed(true), CORES, () -> 1000L);
    Scheduler ioScheduler = service.createIoScheduler(config(), CORES, () -> 1000L);

    Latch latch = new Latch();

    // Fill up the IO pool
    for (int i = 0; i < threadPoolsConfig.getIoMaxPoolSize().getAsInt(); ++i) {
      consumeThread(ioScheduler, latch);
    }

    Future<Boolean> submitted = customScheduler.submit(() -> {
      ioScheduler.submit(() -> {
      });

      fail("Didn't wait");
      return null;
    });

    // Assert that the task is waiting
    expected.expect(TimeoutException.class);
    try {
      submitted.get(5, SECONDS);
    } finally {
      latch.countDown();
      ioScheduler.shutdown();
    }
  }

  @Test
  @Description("Tests that when the CPU-lite pool is full, any task dispatched from a CUSTOM pool with DirectRunToFullCpuLight falg to CPU-lite is run directlyi in the caller thread.")
  public void customDirectRunToFullCpuLight() throws InterruptedException, ExecutionException, TimeoutException {
    assumeDedicatedStrategy();

    Scheduler customScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1).withDirectRunCpuLightWhenTargetBusy(true), CORES,
                                      () -> 1000L);
    Scheduler cpuLightScheduler = service.createCpuLightScheduler(config(), CORES, () -> 1000L);

    Latch latch = new Latch();

    // Fill up the CPU-lite pool
    for (int i = 0; i < threadPoolsConfig.getCpuLightPoolSize().getAsInt()
        + threadPoolsConfig.getCpuLightQueueSize().getAsInt(); ++i) {
      consumeThread(cpuLightScheduler, latch);
    }

    AtomicReference<Thread> callerThread = new AtomicReference<>();
    AtomicReference<Thread> taskRunThread = new AtomicReference<>();

    Future<Boolean> submitted = customScheduler.submit(() -> {
      callerThread.set(currentThread());

      cpuLightScheduler.submit(() -> {
        taskRunThread.set(currentThread());
      });

      return null;
    });

    try {
      submitted.get(5, SECONDS);
    } finally {
      latch.countDown();
    }

    assertThat(taskRunThread.get(), sameInstance(callerThread.get()));
  }

  @Test
  @Description("Tests that the behavior of combining runCpuLightWhenTargetBusy and waitAllowed depends on the target thread.")
  public void customWaitToFullIoWaitsAndWaitToFullIoWaits() throws InterruptedException, ExecutionException, TimeoutException {
    assumeDedicatedStrategy();

    Scheduler customScheduler = service
        .createCustomScheduler(config().withMaxConcurrentTasks(1).withWaitAllowed(true).withDirectRunCpuLightWhenTargetBusy(true),
                               CORES, () -> 1000L);
    Scheduler ioScheduler = service.createIoScheduler(config(), CORES, () -> 1000L);
    Scheduler cpuLightScheduler = service.createCpuLightScheduler(config(), CORES, () -> 1000L);

    Latch latch = new Latch();

    // Fill up the IO pool
    for (int i = 0; i < threadPoolsConfig.getIoMaxPoolSize().getAsInt(); ++i) {
      consumeThread(ioScheduler, latch);
    }
    // Fill up the CPU-lite pool
    for (int i = 0; i < threadPoolsConfig.getCpuLightPoolSize().getAsInt()
        + threadPoolsConfig.getCpuLightQueueSize().getAsInt(); ++i) {
      consumeThread(cpuLightScheduler, latch);
    }

    AtomicReference<Thread> callerThread = new AtomicReference<>();
    AtomicReference<Thread> taskRunThread = new AtomicReference<>();

    Future<Boolean> submittedCpuLight = customScheduler.submit(() -> {
      callerThread.set(currentThread());

      cpuLightScheduler.submit(() -> {
        taskRunThread.set(currentThread());
      });

      return null;
    });

    Future<Boolean> submittedIo = customScheduler.submit(() -> {
      ioScheduler.submit(() -> {
      });

      fail("Didn't wait");
      return null;
    });

    try {
      submittedCpuLight.get(5, SECONDS);
      assertThat(taskRunThread.get(), sameInstance(callerThread.get()));

      // Asssert that the task is waiting
      expected.expect(TimeoutException.class);
      submittedIo.get(5, SECONDS);
    } finally {
      latch.countDown();
    }
  }

  @Test
  @Description("Tests that periodic tasks scheduled to a busy Scheduler are skipped but the job continues executing.")
  public void rejectionPolicyScheduledPeriodic()
      throws MuleException, InterruptedException, ExecutionException, TimeoutException {
    Scheduler sourceScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(2), CORES, () -> 1000L);
    Scheduler targetScheduler = service.createCpuLightScheduler(config(), CORES, () -> 1000L);

    Latch latch = new Latch();

    Future<Object> submit = sourceScheduler.submit(threadsConsumer(targetScheduler, latch));

    try {
      submit.get(5, SECONDS);
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(SchedulerBusyException.class));
    }

    CountDownLatch scheduledTaskLatch = new CountDownLatch(2);
    AtomicReference<ScheduledFuture> scheduledTask = new AtomicReference<>(null);

    sourceScheduler.submit(() -> {
      scheduledTask.set(targetScheduler.scheduleWithFixedDelay(() -> {
        scheduledTaskLatch.countDown();
      }, 0, 1, SECONDS));
      return null;
    });

    new PollingProber().check(new JUnitLambdaProbe(() -> {
      assertThat(scheduledTask.get().isDone(), is(true));
      return true;
    }));
    latch.countDown();

    assertThat(scheduledTaskLatch.await(5, SECONDS), is(true));
  }

  @Test
  @Description("Tests that tasks dispatched from a Custom scheduler thread to a busy Scheduler waits for execution.")
  public void rejectionPolicyCustom() throws MuleException, InterruptedException, ExecutionException, TimeoutException {
    Scheduler sourceScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);
    Scheduler targetScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);

    Latch latch = new Latch();

    Future<Object> submit = sourceScheduler.submit(threadsConsumer(targetScheduler, latch));

    expected.expect(ExecutionException.class);
    expected.expectCause(instanceOf(SchedulerBusyException.class));
    submit.get(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);
  }

  @Test
  @Description("Tests that tasks scheduled from a Custom scheduler thread are skipped of triggered when the scheduler is busy.")
  public void rejectionPolicyCustomScheduleAtFixedRate()
      throws MuleException, InterruptedException, ExecutionException, TimeoutException {
    Scheduler sourceScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);
    Scheduler targetScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);

    Latch latch = new Latch();

    Future<Object> submit = sourceScheduler.submit(threadsConsumer(targetScheduler, latch));

    expected.expect(ExecutionException.class);
    expected.expectCause(instanceOf(SchedulerBusyException.class));

    try {
      submit.get(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);
    } finally {
      AtomicBoolean scheduledExecuted = new AtomicBoolean();

      targetScheduler.scheduleAtFixedRate(() -> {
        scheduledExecuted.set(true);
      }, 0, 5, SECONDS);

      sleep(1000);
      assertThat(scheduledExecuted.get(), is(false));

      latch.countDown();

      probe(6000, 500, () -> scheduledExecuted.get());
    }
  }

  @Test
  @Description("Tests that tasks scheduled from a Custom scheduler thread are skipped of triggered when the scheduler is busy.")
  public void rejectionPolicyCustomScheduleWithFixedDelay()
      throws MuleException, InterruptedException, ExecutionException, TimeoutException {
    Scheduler sourceScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);
    Scheduler targetScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);

    Latch latch = new Latch();

    Future<Object> submit = sourceScheduler.submit(threadsConsumer(targetScheduler, latch));

    expected.expect(ExecutionException.class);
    expected.expectCause(instanceOf(SchedulerBusyException.class));

    try {
      submit.get(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);
    } finally {
      AtomicBoolean scheduledExecuted = new AtomicBoolean();

      targetScheduler.scheduleWithFixedDelay(() -> {
        scheduledExecuted.set(true);
      }, 0, 5, SECONDS);

      sleep(1000);
      assertThat(scheduledExecuted.get(), is(false));

      latch.countDown();

      probe(6000, 500, () -> scheduledExecuted.get());
    }
  }

  @Test
  @Description("Tests that tasks dispatched from a Custom scheduler with 'Wait' allowed thread to a busy Scheduler waits for execution.")
  public void rejectionPolicyCustomWithConfig() throws MuleException, InterruptedException, ExecutionException, TimeoutException {
    Scheduler sourceScheduler = service.createCustomScheduler(config().withWaitAllowed(true).withMaxConcurrentTasks(1),
                                                              CORES, () -> 1000L, 1);
    Scheduler targetScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);

    Latch latch = new Latch();

    Future<Object> submit = sourceScheduler.submit(threadsConsumer(targetScheduler, latch));

    try {
      submit.get(1, SECONDS);
      fail();
    } catch (TimeoutException te) {
    }

    latch.countDown();
    submit.get(5, SECONDS);
  }

  @Test
  @Description("Tests that tasks dispatched from any other thread to a busy Scheduler are rejected.")
  public void rejectionPolicyOther() throws MuleException, InterruptedException, ExecutionException, TimeoutException {
    ExecutorService sourceExecutor = newSingleThreadExecutor();
    Scheduler targetScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);

    Latch latch = new Latch();

    Future<Object> submit = sourceExecutor.submit(threadsConsumer(targetScheduler, latch));

    try {
      submit.get(1, SECONDS);
      fail();
    } catch (TimeoutException te) {
    }

    latch.countDown();
    submit.get(5, SECONDS);
  }

  @Test
  public void customSchedulerThreadGroupDestroy() throws Exception {
    AtomicReference<ExecutorService> innerExecutor = new AtomicReference<>();
    AtomicBoolean innerThreadInterupted = new AtomicBoolean();
    Scheduler targetScheduler =
        service.createCustomScheduler(config().withMaxConcurrentTasks(1), CORES, () -> 1000L);

    targetScheduler.submit(() -> {
      // threads from innerExecutor will inherit the threadGroup of the custom scheduler...
      innerExecutor.set(newCachedThreadPool());
    });

    probe(() -> innerExecutor.get() != null);

    Latch latch = new Latch();
    innerExecutor.get().submit(() -> {
      try {
        return latch.await(getTestTimeoutSecs(), SECONDS);
      } catch (InterruptedException e) {
        innerThreadInterupted.set(true);
        currentThread().interrupt();
        return false;
      }
    });

    targetScheduler.stop();

    latch.countDown();

    probe(5000, 100, () -> innerThreadInterupted.get());
  }

  @Test
  @Description("Tests that ThrottledScheduler is not used for CPU light schedulers unless maxConcurrency is less than backing pool max size.")
  public void maxCpuLightConcurrencyMoreThanMaxPoolSizeDoesntUseThrottlingScheduler() {
    assumeDedicatedStrategy();

    assertThat(service
        .createCpuLightScheduler(config().withMaxConcurrentTasks(threadPoolsConfig.getCpuLightPoolSize().getAsInt()), 1,
                                 () -> 1l),
               not(instanceOf(ThrottledScheduler.class)));
    assertThat(service
        .createCpuLightScheduler(config().withMaxConcurrentTasks(threadPoolsConfig.getCpuLightPoolSize().getAsInt()
            - 1), 1,
                                 () -> 1l),
               instanceOf(ThrottledScheduler.class));
  }

  @Test
  @Description("Tests that ThrottledScheduler is not used for CPU intensive schedulers unless maxConcurrency is less than backing pool max size.")
  public void maxCpuIntensiveConcurrencyMoreThanMaxPoolSizeDoesntUseThrottlingScheduler() {
    assumeDedicatedStrategy();
    assertThat(service
        .createCpuIntensiveScheduler(config().withMaxConcurrentTasks(threadPoolsConfig
            .getCpuIntensivePoolSize().getAsInt()), 1,
                                     () -> 1l),
               not(instanceOf(ThrottledScheduler.class)));
    assertThat(service
        .createCpuIntensiveScheduler(config().withMaxConcurrentTasks(threadPoolsConfig.getCpuIntensivePoolSize().getAsInt()
            - 1), 1,
                                     () -> 1l),
               instanceOf(ThrottledScheduler.class));
  }

  @Test
  @Description("Tests that ThrottledScheduler is not used for IO schedulers unless maxConcurrency is less than backing pool max size.")
  public void maxIOConcurrencyMoreThanMaxPoolSizeDoesntUseThrottlingScheduler() {
    assumeDedicatedStrategy();

    assertThat(service
        .createIoScheduler(config().withMaxConcurrentTasks(threadPoolsConfig
            .getIoMaxPoolSize().getAsInt()), 1,
                           () -> 1l),
               not(instanceOf(ThrottledScheduler.class)));
    assertThat(service
        .createIoScheduler(config().withMaxConcurrentTasks(threadPoolsConfig.getIoMaxPoolSize().getAsInt()
            - 1), 1,
                           () -> 1l),
               instanceOf(ThrottledScheduler.class));
  }

  private Callable<Object> threadsConsumer(Scheduler targetScheduler, Latch latch) {
    return () -> {
      while (latch.getCount() > 0) {
        consumeThread(targetScheduler, latch);
      }
      return null;
    };
  }

  private void consumeThread(Scheduler scheduler, Latch latch) {
    scheduler.submit(() -> {
      awaitLatch(latch);
    });
  }

  private boolean awaitLatch(Latch latch) {
    try {
      return latch.await(getTestTimeoutSecs(), SECONDS);
    } catch (InterruptedException e) {
      currentThread().interrupt();
      return false;
    }
  }

  private void assumeDedicatedStrategy() {
    assumeThat(strategy, is(DEDICATED));
  }
}
