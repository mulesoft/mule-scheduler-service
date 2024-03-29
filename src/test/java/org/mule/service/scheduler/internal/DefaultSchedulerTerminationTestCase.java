/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.defaultThreadFactory;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SchedulerServiceStory.TERMINATION;

import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.tck.probe.JUnitLambdaProbe;
import org.mule.tck.probe.PollingProber;

import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;

@RunWith(Parameterized.class)
@Feature(SCHEDULER_SERVICE)
@Story(TERMINATION)
public class DefaultSchedulerTerminationTestCase extends BaseDefaultSchedulerTestCase {

  private static final long STOP_DELAY_DELTA = 100l;

  private Matcher<ExecutorService> terminatedMatcher;

  public DefaultSchedulerTerminationTestCase(Matcher<ExecutorService> terminatedMatcher) {
    this.terminatedMatcher = terminatedMatcher;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return asList(new Object[][] {
        {isTerminated()},
        {isTerminatedAfterAwait()}
    });
  }

  @Test
  @Description("Tests that the Scheduler is properly terminated after calling shutdown()")
  public void terminatedAfterShutdownSameExecutor() throws InterruptedException, ExecutionException {
    final ScheduledExecutorService executor = createExecutor();

    executor.shutdown();

    assertThat(executor, terminatedMatcher);
  }

  @Test
  @Description("Tests that calling shutdown() in a Scheduler has no impact on another Scheduler backed by the same Executor")
  public void terminatedAfterShutdownOtherExecutor() throws InterruptedException, ExecutionException {
    final ScheduledExecutorService executor1 = createExecutor();
    final ScheduledExecutorService executor2 = createExecutor();

    executor1.shutdown();

    assertThat(executor1, terminatedMatcher);
    assertThat(executor2, not(terminatedMatcher));
  }

  @Test
  @Description("Tests that the Scheduler is properly terminated after calling shutdownNow()")
  public void terminatedAfterShutdownNowSameExecutor() throws InterruptedException, ExecutionException {
    final ScheduledExecutorService executor = createExecutor();

    executor.shutdownNow();

    assertThat(executor, terminatedMatcher);
  }

  @Test
  @Description("Tests that calling shutdownNow() in a Scheduler has no impact on another Scheduler backed by the same Executor")
  public void terminatedAfterShutdownNowOtherExecutor() throws InterruptedException, ExecutionException {
    final ScheduledExecutorService executor1 = createExecutor();
    final ScheduledExecutorService executor2 = createExecutor();

    executor1.shutdownNow();

    assertThat(executor1, isTerminated());
    assertThat(executor2, not(terminatedMatcher));
  }

  @Test
  @Description("Tests that calling shutdown() on a Scheduler while it's running a task waits for it to finish before terminating")
  public void terminatedAfterShutdownRunningTask() throws InterruptedException, ExecutionException, TimeoutException {
    final ScheduledExecutorService executor = createExecutor();

    final CountDownLatch latch = new CountDownLatch(1);

    final Future<Boolean> result = executor.submit(() -> {
      return awaitLatch(latch);
    });

    executor.shutdown();

    assertThat(executor, not(terminatedMatcher));
    latch.countDown();
    result.get(EXECUTOR_TIMEOUT_SECS, SECONDS);

    // Due to how threads are scheduled, the termination state of the executor may become available after the getter of the future
    // returns.
    terminationProber().check(new JUnitLambdaProbe(() -> {
      assertThat(executor, terminatedMatcher);
      return true;
    }));
  }

  @Test
  @Description("Tests that calling shutdownNow() on a Scheduler terminates it even if it's running a submitted task")
  public void terminatedAfterShutdownNowRunningSubmittedTask() throws InterruptedException, ExecutionException {
    final ScheduledExecutorService executor = createExecutor();

    final CountDownLatch latch = new CountDownLatch(1);

    executor.submit(() -> {
      return awaitLatch(latch);
    });

    executor.shutdownNow();

    assertThat(executor, terminatedMatcher);
  }

  @Test
  @Description("Tests that calling shutdownNow() on a Scheduler terminates it even if it's running a task")
  public void terminatedAfterShutdownNowRunningTask() throws InterruptedException, ExecutionException {
    final ScheduledExecutorService executor = createExecutor();

    final CountDownLatch latch = new CountDownLatch(1);

    executor.execute(() -> {
      awaitLatch(latch);
    });

    executor.shutdownNow();

    assertThat(executor, terminatedMatcher);
  }

  @Test
  @Description("Tests that calling shutdown() on a Scheduler with a queued task runs that task before terminating")
  public void terminatedAfterShutdownPendingTask() throws InterruptedException, ExecutionException, TimeoutException {
    final ScheduledExecutorService executor = createExecutor();

    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    final Future<Boolean> result = executor.submit(() -> {
      return awaitLatch(latch1);
    });
    final Future<Boolean> pendingResult = executor.submit(() -> {
      return awaitLatch(latch2);
    });

    executor.shutdown();

    assertThat(executor, not(terminatedMatcher));
    latch1.countDown();
    assertThat(result.get(EXECUTOR_TIMEOUT_SECS, SECONDS), is(true));
    assertThat(executor, not(terminatedMatcher));
    latch2.countDown();
    assertThat(pendingResult.get(EXECUTOR_TIMEOUT_SECS, SECONDS), is(true));

    // Due to how threads are scheduled, the termination state of the executor may become available after the getter of the future
    // returns.
    terminationProber().check(new JUnitLambdaProbe(() -> {
      assertThat(executor, terminatedMatcher);
      return true;
    }));
  }

  @Test
  @Description("Tests that calling shutdown() on a Scheduler with a fixed-delay Callable in-between executions frees the timer executor")
  public void terminatedAfterShutdownInBetweenFixedDelayTask() throws InterruptedException, ExecutionException, TimeoutException {
    final ScheduledExecutorService executor = createExecutor();

    final CountDownLatch latch = new CountDownLatch(1);

    final ScheduledFuture<?> result = executor.scheduleWithFixedDelay(() -> {
      latch.countDown();
    }, 0, 10, SECONDS);

    latch.await();
    result.cancel(false);

    assertTerminationIsNotDelayed(sharedScheduledExecutor);

    executor.shutdown();
    // Due to how threads are scheduled, the termination state of the executor may become available after the getter of the future
    // returns.
    terminationProber().check(new JUnitLambdaProbe(() -> {
      assertThat(executor, terminatedMatcher);
      return true;
    }));
  }

  @Test
  @Description("Tests that calling shutdownNow() on a Scheduler with a queued submitted task doesn't wait for that task to run before terminating")
  public void terminatedAfterShutdownNowPendingSubmittedTask() throws InterruptedException, ExecutionException {
    final ScheduledExecutorService executor = createExecutor();

    final CountDownLatch latch = new CountDownLatch(1);

    executor.submit(() -> {
      return awaitLatch(latch);
    });
    executor.submit(() -> {
      return awaitLatch(latch);
    });

    executor.shutdownNow();

    assertThat(executor, terminatedMatcher);
  }

  @Test
  @Description("Tests that calling shutdownNow() on a Scheduler with a queued task doesn't wait for that task to run before terminating")
  public void terminatedAfterShutdownNowPendingTask() throws InterruptedException, ExecutionException {
    final ScheduledExecutorService executor = createExecutor();

    final CountDownLatch latch = new CountDownLatch(1);

    executor.execute(() -> {
      awaitLatch(latch);
    });
    executor.execute(() -> {
      awaitLatch(latch);
    });

    executor.shutdownNow();

    assertThat(executor, terminatedMatcher);
  }

  @Test
  @Description("Tests that the Scheduler is gracefully terminated after calling stop()")
  public void terminatedAfterStopGracefully() throws InterruptedException, ExecutionException {
    final Scheduler executor = (Scheduler) createExecutor();

    sharedExecutor.submit(() -> executor.stop());

    terminationProber().check(new JUnitLambdaProbe(() -> {
      assertThat(executor, terminatedMatcher);
      return true;
    }));
  }

  @Test
  @Description("Tests that calling stop() on a Scheduler while it's running a submitted task waits for it to finish before terminating gracefully")
  public void terminatedAfterStopGracefullyRunningSubmittedTask()
      throws InterruptedException, ExecutionException, TimeoutException {
    final Scheduler executor = (Scheduler) createExecutor();

    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    executor.submit(() -> {
      latch1.countDown();
      return awaitLatch(latch2);
    });

    latch1.await(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);

    final ExecutorService auxExecutor = newSingleThreadExecutor();
    try {
      auxExecutor.submit(() -> executor.stop());
      latch2.countDown();

      terminationProber().check(new JUnitLambdaProbe(() -> {
        assertThat(executor, terminatedMatcher);
        return true;
      }));
    } finally {
      auxExecutor.shutdown();
    }
  }

  @Test
  @Description("Tests that calling stop() on a Scheduler while it's running a task waits for it to finish before terminating gracefully")
  public void terminatedAfterStopGracefullyRunningTask() throws InterruptedException, ExecutionException, TimeoutException {
    final Scheduler executor = (Scheduler) createExecutor();

    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    executor.execute(() -> {
      latch1.countDown();
      awaitLatch(latch2);
    });

    latch1.await(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);

    final ExecutorService auxExecutor = newSingleThreadExecutor();
    try {
      auxExecutor.submit(() -> executor.stop());
      latch2.countDown();

      terminationProber().check(new JUnitLambdaProbe(() -> {
        assertThat(executor, terminatedMatcher);
        return true;
      }));
    } finally {
      auxExecutor.shutdown();
    }
  }

  @Test
  @Description("Tests that calling stop() on a Scheduler with a queued submitted task runs that task before terminating gracefully")
  public void terminatedAfterStopGracefullyPendingSubmittedTask()
      throws InterruptedException, ExecutionException, TimeoutException {
    final Scheduler executor = (Scheduler) createExecutor();

    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    executor.submit(() -> {
      return awaitLatch(latch1);
    });
    executor.submit(() -> {
      return awaitLatch(latch2);
    });

    final ExecutorService auxExecutor = newSingleThreadExecutor();
    try {
      auxExecutor.submit(() -> executor.stop());

      latch1.countDown();
      latch2.countDown();

      terminationProber().check(new JUnitLambdaProbe(() -> {
        assertThat(executor, terminatedMatcher);
        return true;
      }));
    } finally {
      auxExecutor.shutdown();
    }
  }

  @Test
  @Description("Tests that calling stop() on a Scheduler with a queued task runs that task before terminating gracefully")
  public void terminatedAfterStopGracefullyPendingTask() throws InterruptedException, ExecutionException, TimeoutException {
    final Scheduler executor = (Scheduler) createExecutor();

    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    executor.execute(() -> {
      awaitLatch(latch1);
    });
    executor.execute(() -> {
      awaitLatch(latch2);
    });

    final ExecutorService auxExecutor = newSingleThreadExecutor();
    try {
      auxExecutor.submit(() -> executor.stop());

      latch1.countDown();
      latch2.countDown();

      terminationProber().check(new JUnitLambdaProbe(() -> {
        assertThat(executor, terminatedMatcher);
        return true;
      }));
    } finally {
      auxExecutor.shutdown();
    }
  }

  @Test
  @Description("Tests that calling stop() on a Scheduler after running a submitted task terminates gracefullyand immediately")
  public void terminatedAfterStopGracefullyFinishedSubmittedTask()
      throws InterruptedException, ExecutionException, TimeoutException {
    final Scheduler executor = (Scheduler) createExecutor();

    executor.submit(() -> {
      return true;
    });

    final ExecutorService auxExecutor = newSingleThreadExecutor();
    try {
      final long stopReqNanos = nanoTime();
      auxExecutor.submit(() -> executor.stop());
      terminationProber().check(new JUnitLambdaProbe(() -> {
        assertThat(executor, terminatedMatcher);
        return true;
      }));
      assertThat(NANOSECONDS.toMillis(nanoTime() - stopReqNanos), lessThan(STOP_DELAY_DELTA));
    } finally {
      auxExecutor.shutdown();
    }
  }

  @Test
  @Description("Tests that calling stop() on a Scheduler after running a task terminates gracefullyand immediately")
  public void terminatedAfterStopGracefullyFinishedTask() throws InterruptedException, ExecutionException, TimeoutException {
    final Scheduler executor = (Scheduler) createExecutor();

    executor.execute(() -> {
    });

    final ExecutorService auxExecutor = newSingleThreadExecutor();
    try {
      final long stopReqNanos = nanoTime();
      auxExecutor.submit(() -> executor.stop());
      terminationProber().check(new JUnitLambdaProbe(() -> {
        assertThat(executor, terminatedMatcher);
        return true;
      }));
      assertThat(NANOSECONDS.toMillis(nanoTime() - stopReqNanos), lessThan(STOP_DELAY_DELTA));
    } finally {
      auxExecutor.shutdown();
    }
  }

  @Test
  @Description("Tests that calling stop() on a Scheduler while it's running a submitted task forcefully terminates it")
  public void terminatedAfterStopForcefullyRunningSubmittedTask()
      throws InterruptedException, ExecutionException, TimeoutException {
    final Scheduler executor = (Scheduler) createExecutor();

    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    AtomicReference<Thread> taskThread = new AtomicReference<>();

    executor.submit(() -> {
      latch1.countDown();
      try {
        return awaitLatch(latch2);
      } finally {
        if (currentThread().isInterrupted()) {
          taskThread.set(currentThread());
        }
      }
    });

    latch1.await(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);

    executor.stop();

    terminationProber().check(new JUnitLambdaProbe(() -> {
      assertThat(taskThread.get(), is(not(nullValue())));
      assertThat(executor, terminatedMatcher);
      return true;
    }));
  }

  @Test
  @Description("Tests that calling stop() on a Scheduler while it's running a task forcefully terminates it")
  public void terminatedAfterStopForcefullyRunningTask() throws InterruptedException, ExecutionException, TimeoutException {
    final Scheduler executor = (Scheduler) createExecutor();

    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    AtomicReference<Thread> taskThread = new AtomicReference<>();

    executor.execute(() -> {
      latch1.countDown();
      try {
        awaitLatch(latch2);
      } finally {
        if (currentThread().isInterrupted()) {
          taskThread.set(currentThread());
        }
      }
    });

    latch1.await(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);

    executor.stop();

    terminationProber().check(new JUnitLambdaProbe(() -> {
      assertThat(taskThread.get(), is(not(nullValue())));
      assertThat(executor, terminatedMatcher);
      return true;
    }));
  }

  @Test
  @Description("Tests that calling stop() on a Scheduler while it's running 2 tasks forcefully terminates them")
  public void terminatedAfterStopForcefullyRunningTasks() throws InterruptedException, ExecutionException, TimeoutException {
    sharedExecutor.shutdownNow();
    sharedExecutor = new ThreadPoolExecutor(5, 5, 0, SECONDS, sharedExecutorQueue, defaultThreadFactory());

    final Scheduler executor = (Scheduler) createExecutor();

    final CountDownLatch latch1 = new CountDownLatch(5);
    final CountDownLatch latch2 = new CountDownLatch(1);

    AtomicInteger interruptedThreads = new AtomicInteger();

    for (int i = 0; i < 5; ++i) {
      executor.execute(() -> {
        latch1.countDown();
        try {
          awaitLatch(latch2);
        } finally {
          if (currentThread().isInterrupted()) {
            interruptedThreads.incrementAndGet();
          }
        }
      });
    }

    latch1.await(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);

    executor.stop();

    terminationProber().check(new JUnitLambdaProbe(() -> {
      assertThat(interruptedThreads.get(), is(5));
      assertThat(executor, terminatedMatcher);
      return true;
    }));
  }

  @Test
  @Description("Tests that calling stop() on a Scheduler with a queued submitted task forcefully terminates it")
  public void terminatedAfterStopForcefullyPendingSubmittedTask()
      throws InterruptedException, ExecutionException, TimeoutException {
    final Scheduler executor = (Scheduler) createExecutor();

    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch finallyLatch = new CountDownLatch(2);

    AtomicReference<Thread> taskThread = new AtomicReference<>();
    AtomicReference<Thread> pendingTaskThread = new AtomicReference<>();

    executor.submit(() -> {
      try {
        return awaitLatch(latch);
      } finally {
        if (currentThread().isInterrupted()) {
          taskThread.set(currentThread());
        }
        finallyLatch.countDown();
      }
    });
    executor.submit(() -> {
      try {
        return awaitLatch(latch);
      } finally {
        if (currentThread().isInterrupted()) {
          pendingTaskThread.set(currentThread());
        }
        finallyLatch.countDown();
      }
    });

    executor.stop();

    finallyLatch.await(2, SECONDS);
    assertThat(taskThread.get(), is(not(nullValue())));
    assertThat(pendingTaskThread.get(), is(nullValue()));

    terminationProber().check(new JUnitLambdaProbe(() -> {
      assertThat(executor, terminatedMatcher);
      return true;
    }));
  }

  @Test
  @Description("Tests that calling stop() on a Scheduler with a queued task forcefully terminates it")
  public void terminatedAfterStopForcefullyPendingTask() throws InterruptedException, ExecutionException, TimeoutException {
    final Scheduler executor = (Scheduler) createExecutor();

    final CountDownLatch latch = new CountDownLatch(1);

    AtomicReference<Thread> taskThread = new AtomicReference<>();
    AtomicReference<Thread> pendingTaskThread = new AtomicReference<>();

    executor.execute(() -> {
      try {
        awaitLatch(latch);
      } finally {
        if (currentThread().isInterrupted()) {
          taskThread.set(currentThread());
        }
      }
    });
    executor.execute(() -> {
      try {
        awaitLatch(latch);
      } finally {
        if (currentThread().isInterrupted()) {
          pendingTaskThread.set(currentThread());
        }
      }
    });

    executor.stop();

    terminationProber().check(new JUnitLambdaProbe(() -> {
      assertThat(taskThread.get(), is(not(nullValue())));
      assertThat(pendingTaskThread.get(), is(nullValue()));
      assertThat(executor, terminatedMatcher);
      return true;
    }));
  }

  private PollingProber terminationProber() {
    return new PollingProber(500, 50);
  }

  private static Matcher<ExecutorService> isTerminated() {
    return new TypeSafeMatcher<ExecutorService>() {

      private String itemString;

      @Override
      protected boolean matchesSafely(ExecutorService item) {
        this.itemString = item.toString();
        return item.isTerminated();
      }

      @Override
      public void describeTo(org.hamcrest.Description description) {
        description.appendValue(itemString);
      }
    };
  }

  private static Matcher<ExecutorService> isTerminatedAfterAwait() {
    return new TypeSafeMatcher<ExecutorService>() {

      private String itemString;

      @Override
      protected boolean matchesSafely(ExecutorService item) {
        this.itemString = item.toString();
        try {
          return item.awaitTermination(EXECUTOR_TIMEOUT_SECS, SECONDS);
        } catch (InterruptedException e) {
          currentThread().interrupt();
          return false;
        }
      }

      @Override
      public void describeTo(org.hamcrest.Description description) {
        description.appendValue(itemString);
      }
    };
  }
}
