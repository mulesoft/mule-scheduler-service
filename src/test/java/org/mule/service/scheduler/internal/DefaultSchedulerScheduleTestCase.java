/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SchedulerServiceStory.TASK_SCHEDULING;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.junit.Test;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;

@Feature(SCHEDULER_SERVICE)
@Story(TASK_SCHEDULING)
public class DefaultSchedulerScheduleTestCase extends AbstractMuleVsJavaExecutorTestCase {

  private static final long TASK_DURATION_MILLIS = 200;
  private static final long TEST_DELAY_MILLIS = 1000;

  public DefaultSchedulerScheduleTestCase(Function<AbstractMuleVsJavaExecutorTestCase, ScheduledExecutorService> executorFactory,
                                          BlockingQueue<Runnable> sharedExecutorQueue, String param) {
    super(executorFactory, sharedExecutorQueue, param);
  }

  @Test
  @Description("Tests scheduling a Runnable in the future")
  public void scheduleRunnable() throws InterruptedException, ExecutionException, TimeoutException {
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    final long scheduleNanos = System.nanoTime();
    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      latch2.countDown();
      awaitLatch(latch1);
    }, 1, SECONDS);

    assertThat(latch2.await(2 * EXECUTOR_TIMEOUT_SECS, SECONDS), is(true));
    latch1.countDown();
    scheduled.get(2 * EXECUTOR_TIMEOUT_SECS, SECONDS);
    final long finishNanos = System.nanoTime();
    assertThat(finishNanos - scheduleNanos, greaterThanOrEqualTo(SECONDS.toNanos(1)));
  }

  @Test
  @Description("Tests that calling get on a ScheduledFuture with a time lower than the duration of the Runnable task throws a TimeoutException")
  public void scheduleRunnableGetTimeout() throws InterruptedException, ExecutionException, TimeoutException {
    final CountDownLatch latch1 = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      awaitLatch(latch1);
    }, 1, SECONDS);

    expected.expect(TimeoutException.class);

    scheduled.get(EXECUTOR_TIMEOUT_SECS, SECONDS);
  }

  @Test
  @Description("Tests scheduling a Callable in the future")
  public void scheduleCallable() throws InterruptedException, ExecutionException, TimeoutException {
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    final long scheduleNanos = System.nanoTime();
    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      latch2.countDown();
      return awaitLatch(latch1);
    }, 1, SECONDS);

    assertThat(latch2.await(2 * EXECUTOR_TIMEOUT_SECS, SECONDS), is(true));
    latch1.countDown();
    scheduled.get(2 * EXECUTOR_TIMEOUT_SECS, SECONDS);
    final long finishNanos = System.nanoTime();
    assertThat(finishNanos - scheduleNanos, greaterThanOrEqualTo(SECONDS.toNanos(1)));
  }

  @Test
  @Description("Tests that calling get on a ScheduledFuture with a time lower than the duration of the Callable task throws a TimeoutException")
  public void scheduleCallableGetTimeout() throws InterruptedException, ExecutionException, TimeoutException {
    final CountDownLatch latch1 = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      return awaitLatch(latch1);
    }, 1, SECONDS);

    expected.expect(TimeoutException.class);

    scheduled.get(EXECUTOR_TIMEOUT_SECS, SECONDS);
  }

  @Test
  @Description("Tests that calling shutdown() on a Scheduler with a Runnable scheduled in the future will wait for that task to finish")
  public void scheduleRunnableShutdownBeforeFire() throws InterruptedException, ExecutionException, TimeoutException {
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    final long scheduleNanos = System.nanoTime();
    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      assertThat(executor.isShutdown(), is(true));
      latch2.countDown();
      awaitLatch(latch1);
    }, 1, SECONDS);

    executor.shutdown();

    assertThat(latch2.await(2 * EXECUTOR_TIMEOUT_SECS, SECONDS), is(true));
    latch1.countDown();
    scheduled.get(2 * EXECUTOR_TIMEOUT_SECS, SECONDS);
    final long finishNanos = System.nanoTime();
    assertThat(finishNanos - scheduleNanos, greaterThanOrEqualTo(SECONDS.toNanos(1)));
  }

  @Test
  @Description("Tests that calling shutdown() on a Scheduler with a Callable scheduled in the future will wait for that task to finish")
  public void scheduleCallableShutdownBeforeFire() throws InterruptedException, ExecutionException, TimeoutException {
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    final long scheduleNanos = System.nanoTime();
    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      assertThat(executor.isShutdown(), is(true));
      latch2.countDown();
      return awaitLatch(latch1);
    }, 1, SECONDS);

    executor.shutdown();

    assertThat(latch2.await(2 * EXECUTOR_TIMEOUT_SECS, SECONDS), is(true));
    latch1.countDown();
    scheduled.get(2 * EXECUTOR_TIMEOUT_SECS, SECONDS);
    final long finishNanos = System.nanoTime();
    assertThat(finishNanos - scheduleNanos, greaterThanOrEqualTo(SECONDS.toNanos(1)));
  }

  @Test
  @Description("Tests that calling shutdownNow() on a Scheduler with a Runnable scheduled in the future will cancel that task")
  public void scheduleRunnableShutdownNowBeforeFire() throws InterruptedException, ExecutionException, TimeoutException {
    executor.schedule(() -> {
      fail("Called after shutdown");
    }, TEST_DELAY_MILLIS, MILLISECONDS);

    assertThat(executor.shutdownNow(), hasSize(1));
  }

  @Test
  @Description("Tests that calling shutdownNow() on a Scheduler with a Callable scheduled in the future will cancel that task")
  public void scheduleCallableShutdownNowBeforeFire() throws InterruptedException, ExecutionException, TimeoutException {
    executor.schedule(() -> {
      fail("Called after shutdown");
    }, 1, SECONDS);

    assertThat(executor.shutdownNow(), hasSize(1));
  }

  @Test
  @Description("Tests that a ScheduledFuture is properly cancelled for a one-shot Runnable before it starts executing")
  public void cancelRunnableBeforeFire() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      awaitLatch(latch);
    }, DEFAULT_TEST_TIMEOUT_SECS, SECONDS);

    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  @Test
  @Description("Tests that a ScheduledFuture is properly cancelled for a one-shot Runnable while it's executing")
  public void cancelRunnableWhileRunning() throws InterruptedException {
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      latch1.countDown();
      awaitLatch(latch2);
    }, 1, SECONDS);

    latch1.await();
    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  @Test
  @Description("Tests that a delayed task is eventually triggered if at the original trigger time the target scheduler was busy")
  public void scheduleWhileBusy() throws InterruptedException, ExecutionException, TimeoutException {
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    try {
      // Use up all available threads
      for (int i = 0; i < 20; ++i) {
        executor.execute(() -> {
          try {
            latch1.await();
          } catch (InterruptedException e) {
            currentThread().interrupt();
          }
        });
      }
    } catch (RejectedExecutionException e) {
      // Nothing to do
    }

    executor.schedule(() -> {
      latch2.countDown();
    }, 1, SECONDS);

    // Wait for the time when the scheduled task would be triggered
    Thread.sleep(2000);

    latch1.countDown();
    assertThat(latch2.await(5, SECONDS), is(true));
  }

  @Test
  @Description("Tests that a ScheduledFuture is properly cancelled for a one-shot Callable before it starts executing")
  public void cancelCallableBeforeFire() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      return awaitLatch(latch);
    }, DEFAULT_TEST_TIMEOUT_SECS, SECONDS);

    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  @Test
  @Description("Tests that a ScheduledFuture is properly cancelled for a one-shot Callable while it's executing")
  public void cancelCallableWhileRunning() throws InterruptedException {
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      latch1.countDown();
      return awaitLatch(latch2);
    }, TEST_DELAY_MILLIS, MILLISECONDS);

    latch1.await();
    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  @Test
  @Description("Tests that a ScheduledFuture is properly cancelled for a fixed-rate Callable before it starts executing")
  public void cancelFixedRateBeforeFire() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.scheduleAtFixedRate(() -> {
      awaitLatch(latch);
    }, SECONDS.toMillis(DEFAULT_TEST_TIMEOUT_SECS), 10 * TEST_DELAY_MILLIS, MILLISECONDS);

    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  @Test
  @Description("Tests that a ScheduledFuture is properly cancelled for a fixed-rate Callable while it's executing")
  public void cancelFixedRateWhileRunning() throws InterruptedException {
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.scheduleAtFixedRate(() -> {
      latch1.countDown();
      awaitLatch(latch2);
    }, TEST_DELAY_MILLIS, 10 * TEST_DELAY_MILLIS, MILLISECONDS);

    latch1.await();
    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  @Test
  @Description("Tests that a ScheduledFuture is properly cancelled for a fixed-rate Callable in-between executions")
  public void cancelFixedRateInBetweenRuns() throws InterruptedException, ExecutionException {
    final CountDownLatch latch = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.scheduleAtFixedRate(() -> {
      sharedScheduledExecutor.schedule(() -> latch.countDown(), 0, SECONDS);
    }, TEST_DELAY_MILLIS, 10 * TEST_DELAY_MILLIS, MILLISECONDS);

    latch.await();
    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  @Test
  @Description("Tests that a ScheduledFuture is properly cancelled for a fixed-delay Callable before it starts executing")
  public void cancelFixedDelayBeforeFire() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.scheduleWithFixedDelay(() -> {
      awaitLatch(latch);
    }, DEFAULT_TEST_TIMEOUT_SECS, 10 * TEST_DELAY_MILLIS, MILLISECONDS);

    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  @Test
  @Description("Tests that a ScheduledFuture is properly cancelled for a fixed-delay Callable while it's executing")
  public void cancelFixedDelayWhileRunning() throws InterruptedException {
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.scheduleWithFixedDelay(() -> {
      latch1.countDown();
      awaitLatch(latch2);
    }, TEST_DELAY_MILLIS, 10 * TEST_DELAY_MILLIS, MILLISECONDS);

    latch1.await();
    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  @Test
  @Description("Tests that a ScheduledFuture is properly cancelled for a fixed-delay Callable in-between executions")
  public void cancelFixedDelayInBetweenRuns() throws InterruptedException, ExecutionException {
    final CountDownLatch latch = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.scheduleWithFixedDelay(() -> {
      sharedScheduledExecutor.schedule(() -> latch.countDown(), 0, SECONDS);
    }, 0, DEFAULT_TEST_TIMEOUT_SECS, SECONDS);

    latch.await();
    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  private void assertCancelled(final ScheduledFuture<?> scheduled) {
    assertThat(scheduled.isCancelled(), is(true));
    assertThat(scheduled.isDone(), is(true));
  }

  @Test
  @Description("Tests that shutdownNow after cancelling a running ScheduledFuture before being fired returns the cancelled task")
  public void shutdownNowAfterCancelCallableBeforeFire() {
    final CountDownLatch latch = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      return awaitLatch(latch);
    }, DEFAULT_TEST_TIMEOUT_SECS, SECONDS);

    scheduled.cancel(true);

    List<Runnable> notStartedTasks = executor.shutdownNow();
    assertThat(notStartedTasks, hasSize(0));
  }

  @Test
  @Description("Tests that shutdownNow after cancelling a running ScheduledFuture after being fired at least once doesn't return the cancelled task")
  public void shutdownNowAfterCancelCallableAfterFire() throws Exception {
    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      return true;
    }, 0, SECONDS);

    scheduled.get(EXECUTOR_TIMEOUT_SECS, SECONDS);
    scheduled.cancel(true);

    List<Runnable> notStartedTasks = executor.shutdownNow();
    assertThat(notStartedTasks, hasSize(0));
  }

  @Test
  @Description("Tests that shutdownNow after cancelling a running ScheduledFuture returns the cancelled task")
  public void shutdownNowAfterCancelCallableWhileRunning() throws InterruptedException {
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.schedule(() -> {
      latch1.countDown();
      return awaitLatch(latch2);
    }, TEST_DELAY_MILLIS, MILLISECONDS);

    latch1.await();
    scheduled.cancel(true);

    executor.shutdownNow();

    List<Runnable> notStartedTasks = executor.shutdownNow();
    assertThat(notStartedTasks, is(empty()));
  }

  @Test
  @Description("Tests that scheduleAtFixedRate parameters are honored")
  public void fixedRateRepeats() {
    List<Long> startTimes = new ArrayList<>();
    List<Long> endTimes = new ArrayList<>();

    final CountDownLatch latch = new CountDownLatch(2);

    final ScheduledFuture<?> scheduled = executor.scheduleAtFixedRate(() -> {
      startTimes.add(System.nanoTime());
      try {
        sleep(TASK_DURATION_MILLIS);
      } catch (InterruptedException e) {
        currentThread().interrupt();
      }
      latch.countDown();
      endTimes.add(System.nanoTime());
    }, 0, TEST_DELAY_MILLIS, MILLISECONDS);

    assertThat(awaitLatch(latch), is(true));
    scheduled.cancel(true);

    verify(sharedScheduledExecutor).scheduleAtFixedRate(any(), eq(0L), eq(TEST_DELAY_MILLIS), eq(MILLISECONDS));
    assertThat(NANOSECONDS.toMillis(startTimes.get(1) - endTimes.get(0)),
               greaterThanOrEqualTo(TEST_DELAY_MILLIS - TASK_DURATION_MILLIS - DELTA_MILLIS));
  }

  @Test
  @Description("Tests that scheduleAtFixedRate parameters are honored even if the task takes longer than the rate")
  public void fixedRateExceeds() {
    List<Long> startTimes = new ArrayList<>();
    List<Long> endTimes = new ArrayList<>();

    final CountDownLatch latch = new CountDownLatch(2);

    final ScheduledFuture<?> scheduled = executor.scheduleAtFixedRate(() -> {
      startTimes.add(System.nanoTime());
      try {
        sleep(TEST_DELAY_MILLIS + TASK_DURATION_MILLIS);
      } catch (InterruptedException e) {
        currentThread().interrupt();
      }
      latch.countDown();
      endTimes.add(System.nanoTime());
    }, 0, TEST_DELAY_MILLIS, MILLISECONDS);

    assertThat(awaitLatch(latch), is(true));
    scheduled.cancel(true);

    assertThat((double) NANOSECONDS.toMillis(startTimes.get(1) - endTimes.get(0)),
               executor instanceof DefaultScheduler && sharedExecutorQueue instanceof SynchronousQueue
                   // A standard java ScheduledExecutor will run the task as soon as possible after the rate has passed.
                   // On mule, for implementation simplicity, if a tick is skipped for a 1-thread scheduler with no queue, the
                   // task won't run until the next tick.
                   ? closeTo(TEST_DELAY_MILLIS - TASK_DURATION_MILLIS, DELTA_MILLIS)
                   : closeTo(0, DELTA_MILLIS));
  }

}
