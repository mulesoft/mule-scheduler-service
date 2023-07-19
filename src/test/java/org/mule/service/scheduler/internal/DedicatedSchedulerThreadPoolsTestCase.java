/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal;

import static org.mule.runtime.api.scheduler.SchedulerConfig.config;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.DEDICATED;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerBusyException;
import org.mule.runtime.api.util.concurrent.Latch;
import org.mule.service.scheduler.internal.util.Delegator;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.Test;

import io.qameta.allure.Description;

public class DedicatedSchedulerThreadPoolsTestCase extends SchedulerThreadPoolsTestCase {

  public DedicatedSchedulerThreadPoolsTestCase() {
    this.strategy = DEDICATED;
  }

  @Test
  @Description("Tests that IO threads in excess of the core size don't hold a reference to an artifact classloader through the inheritedAccessControlContext.")
  public void elasticIoThreadsDontReferenceClassLoaderFromAccessControlContext() throws Exception {
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
  @Description("Tests that tasks dispatched from a CPU Light thread to a busy Scheduler are rejected.")
  public void rejectionPolicyCpuLight() throws MuleException, InterruptedException, ExecutionException, TimeoutException {
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
  @Description("Tests that when the IO pool is full, any task dispatched from IO to IO runs in the caller thread instead of being queued, which can cause a deadlock.")
  public void ioToFullIoDoesntWait() throws InterruptedException, ExecutionException {
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
  @Description("Tests that ThrottledScheduler is not used for CPU light schedulers unless maxConcurrency is less than backing pool max size.")
  public void maxCpuLightConcurrencyMoreThanMaxPoolSizeDoesntUseThrottlingScheduler() {
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

}
