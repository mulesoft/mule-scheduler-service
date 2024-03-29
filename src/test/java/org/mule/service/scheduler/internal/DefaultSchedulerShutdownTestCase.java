/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mule.tck.probe.PollingProber.probe;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SchedulerServiceStory.SHUTDOWN;

import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.util.concurrent.Latch;
import org.mule.tck.probe.JUnitLambdaProbe;
import org.mule.tck.probe.PollingProber;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Test;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import io.qameta.allure.Issue;
import io.qameta.allure.Story;

@Feature(SCHEDULER_SERVICE)
@Story(SHUTDOWN)
public class DefaultSchedulerShutdownTestCase extends BaseDefaultSchedulerTestCase {

  private ScheduledExecutorService executor;
  private ScheduledExecutorService otherExecutor;

  @Override
  public void before() throws Exception {
    super.before();
    executor = createExecutor();
    otherExecutor = createExecutor();
  }

  @Override
  public void after() throws Exception {
    executor.shutdownNow();
    otherExecutor.shutdownNow();
    executor.awaitTermination(5, SECONDS);
    otherExecutor.awaitTermination(5, SECONDS);
    super.after();
  }

  @Test
  @Description("Tests that calling shutdown() on a Scheduler while it's running a task waits for it to finish before terminating")
  public void shutdownWhileRunningTasksFromDifferentSources() throws InterruptedException, ExecutionException, TimeoutException {
    final CountDownLatch latch = new CountDownLatch(1);

    final Future<Boolean> result1 = executor.submit(() -> {
      return awaitLatch(latch);
    });
    final Future<Boolean> result2 = otherExecutor.submit(() -> {
      return awaitLatch(latch);
    });

    otherExecutor.shutdown();

    latch.countDown();

    // Since both tasks where dispatched before calling shutdown, both must finish.
    assertThat(result1.get(EXECUTOR_TIMEOUT_SECS, SECONDS), is(true));
    assertThat(result2.get(EXECUTOR_TIMEOUT_SECS, SECONDS), is(true));
  }

  @Test
  @Description("Tests that calling shutdownNow() on a Scheduler with a queued task cancels that task")
  public void shutdownNowWhileRunningTasksFromDifferentSources()
      throws InterruptedException, ExecutionException, TimeoutException {
    final CountDownLatch latch = new CountDownLatch(1);

    final Future<Boolean> result1 = executor.submit(() -> {
      return awaitLatch(latch);
    });
    final Runnable task2 = () -> {
      awaitLatch(latch);
    };
    otherExecutor.submit(task2);

    final List<Runnable> notStartedTasks = otherExecutor.shutdownNow();

    latch.countDown();

    // Since the first task was sent to executor1, shutting down executor2 must not affect it
    assertThat(result1.get(EXECUTOR_TIMEOUT_SECS, SECONDS), is(true));
    assertThat(notStartedTasks, hasSize(1));
  }

  @Test
  @Description("Tests that a task submitted to a Scheduler after calling shutdown() is rejected")
  public void submitAfterShutdownSameExecutor() throws InterruptedException, ExecutionException {
    executor.shutdown();

    assertRejected(executor, SUBMIT_EMPTY_RUNNABLE);
  }

  @Test
  @Description("Tests that a task submitted to a Scheduler after calling shutdown() on another Scheduler is NOT rejected")
  public void submitAfterShutdownOtherExecutor() throws InterruptedException, ExecutionException, TimeoutException {
    executor.shutdown();

    final CountDownLatch latch = new CountDownLatch(1);

    final Future<Boolean> result = otherExecutor.submit(() -> {
      return awaitLatch(latch);
    });

    latch.countDown();

    assertThat(result.get(EXECUTOR_TIMEOUT_SECS, SECONDS), is(true));
  }

  @Test
  @Description("Tests that a task submitted to a Scheduler after calling shutdownNow() is rejected")
  public void submitAfterShutdownNowSameExecutor() throws InterruptedException, ExecutionException {
    final List<Runnable> notStartedTasks = executor.shutdownNow();

    assertThat(notStartedTasks, is(empty()));

    assertRejected(executor, SUBMIT_EMPTY_RUNNABLE);
  }

  @Test
  @Description("Tests that a task submitted to a Scheduler after calling shutdownNow() on another Scheduler is NOT rejected")
  public void submitAfterShutdownNowOtherExecutor() throws InterruptedException, ExecutionException, TimeoutException {
    executor.shutdownNow();

    final CountDownLatch latch = new CountDownLatch(1);

    final Future<Boolean> result = otherExecutor.submit(() -> {
      return awaitLatch(latch);
    });

    latch.countDown();

    assertThat(result.get(EXECUTOR_TIMEOUT_SECS, SECONDS), is(true));
  }

  @Test
  @Description("Tests that a task submitted to a Scheduler after the service is stopped is rejected")
  public void submitAfterShutdownSharedExecutor() throws InterruptedException, ExecutionException {
    sharedExecutor.shutdown();

    assertRejected(executor, SUBMIT_EMPTY_RUNNABLE);
  }

  @Test
  @Description("Tests that a task submitted to a Scheduler after the service is force-stopped is rejected")
  public void submitAfterShutdownNowSharedExecutor() throws InterruptedException, ExecutionException {
    final List<Runnable> notStartedTasks = sharedExecutor.shutdownNow();

    assertThat(notStartedTasks, is(empty()));

    assertRejected(executor, SUBMIT_EMPTY_RUNNABLE);
  }

  @Test
  @Description("Tests that a running task is interrupted when shutdownNow() is called")
  public void shutdownNowInterruptsTask() throws InterruptedException, ExecutionException {
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch triggeredLatch = new CountDownLatch(1);
    final CountDownLatch interruptionLatch = new CountDownLatch(1);

    final Future<Boolean> result = executor.submit(() -> {
      triggeredLatch.countDown();
      boolean awaited = false;
      try {
        awaited = awaitLatch(latch);
      } finally {
        assertThat(Thread.interrupted(), is(true));
        interruptionLatch.countDown();
        return awaited;
      }
    });

    triggeredLatch.await(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);
    final List<Runnable> notStartedTasks = executor.shutdownNow();
    interruptionLatch.await(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);

    assertThat(notStartedTasks, is(empty()));
    assertThat(result.isCancelled(), is(true));
  }

  @Test
  @Description("Tests that a fixed-rate task stops running when shutdown() is called")
  public void shutdownCancelsFixedRateTasks() throws InterruptedException, ExecutionException {
    AtomicInteger runCount = new AtomicInteger();

    executor.scheduleAtFixedRate(() -> {
      synchronized (runCount) {
        runCount.incrementAndGet();
      }
    }, 0, 1, MILLISECONDS);

    final int runCountBeforeShutdown;
    synchronized (runCount) {
      runCountBeforeShutdown = runCount.get();
      executor.shutdown();
    }

    Thread.sleep(50);

    assertThat(runCount.get(), is(runCountBeforeShutdown));
  }

  @Test
  @Description("Tests that a fixed-delay task stops running when shutdown() is called")
  public void shutdownCancelsFixedDelayTasks() throws InterruptedException, ExecutionException {
    AtomicInteger runCount = new AtomicInteger();

    executor.scheduleWithFixedDelay(() -> {
      synchronized (runCount) {
        runCount.incrementAndGet();
      }
    }, 0, 1, MILLISECONDS);

    final int runCountBeforeShutdown;
    synchronized (runCount) {
      runCountBeforeShutdown = runCount.get();
      executor.shutdown();
    }

    Thread.sleep(50);

    assertThat(runCount.get(), is(runCountBeforeShutdown));
  }

  @Test
  @Description("Tests that a fixed-rate task stops running when shutdownNow() is called")
  public void shutdownNowCancelsFixedRateTasks() throws InterruptedException, ExecutionException {
    AtomicInteger runCount = new AtomicInteger();

    executor.scheduleAtFixedRate(() -> {
      synchronized (runCount) {
        runCount.incrementAndGet();
      }
    }, 0, 1, MILLISECONDS);

    final int runCountBeforeShutdown;
    synchronized (runCount) {
      runCountBeforeShutdown = runCount.get();
      executor.shutdownNow();
    }

    Thread.sleep(50);

    assertThat(runCount.get(), is(runCountBeforeShutdown));
  }

  @Test
  @Description("Tests that a fixed-delay task stops running when shutdownNow() is called")
  public void shutdownNowCancelsFixedDelayTasks() throws InterruptedException, ExecutionException {
    AtomicInteger runCount = new AtomicInteger();

    executor.scheduleWithFixedDelay(() -> {
      synchronized (runCount) {
        runCount.incrementAndGet();
      }
    }, 0, 1, MILLISECONDS);

    final int runCountBeforeShutdown;
    synchronized (runCount) {
      runCountBeforeShutdown = runCount.get();
      executor.shutdownNow();
    }

    Thread.sleep(50);

    assertThat(runCount.get(), is(runCountBeforeShutdown));
  }

  @Test
  @Description("Tests that when a Scheduler with a fixed-delay task is shutdown, is stops rescheduling the task to a terminated executor")
  public void shutdownStopsReschedulingFixedDelayTasks() throws InterruptedException, ExecutionException {
    Latch latch = new Latch();

    executor.scheduleWithFixedDelay(() -> {
      try {
        latch.await();
      } catch (InterruptedException e) {
        currentThread().interrupt();
      }
    }, 0, 1, MILLISECONDS);

    new PollingProber(100, 2).check(new JUnitLambdaProbe(() -> {
      verify(sharedScheduledExecutor).schedule(any(Runnable.class), anyLong(), any());
      return true;
    }));

    reset(sharedScheduledExecutor);
    executor.shutdown();
    latch.countDown();

    Thread.sleep(50);

    verify(sharedScheduledExecutor, never()).schedule(any(Runnable.class), anyLong(), any());
  }

  @Test
  @Issue("MULE-18884")
  public void shutdownFromWithinSchedulerTaskDoesntWait() {
    final Future<?> stopFuture = executor.submit(() -> {
      ((Scheduler) executor).stop();
    });

    // the timeout of this probe has to be way lower that the graceful shutdown timeout of the scheduler for replicating the bug.
    probe(1000, 10, () -> stopFuture.isDone() && executor.isTerminated());
  }

  protected void assertRejected(final ScheduledExecutorService executor,
                                final Consumer<ScheduledExecutorService> submitEmptyRunnable) {
    expected.expect(instanceOf(RejectedExecutionException.class));
    expected.expectMessage(is(executor.toString() + " already shutdown"));
    submitEmptyRunnable.accept(executor);
  }
}
