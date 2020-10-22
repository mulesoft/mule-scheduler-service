/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mule.runtime.api.scheduler.SchedulerConfig.config;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.DEDICATED;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.loadThreadPoolsConfig;
import static org.mule.tck.probe.PollingProber.probe;

import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.util.concurrent.Latch;
import org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig;
import org.mule.service.scheduler.internal.executor.SchedulerTaskThrottledException;
import org.mule.service.scheduler.internal.threads.SchedulerThreadPools;
import org.mule.tck.probe.JUnitLambdaProbe;
import org.mule.tck.probe.PollingProber;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import io.qameta.allure.Issue;

@Feature("Scheduler Throttling")
public class ThrottledSchedulerThrottleTestCase extends BaseDefaultSchedulerTestCase {

  private static final int THROTTLE_SIZE = 2;
  private static final int SINGLE_TASK_THROTTLE_SIZE = 1;
  private ExecutorService outerExecutor;
  private ContainerThreadPoolsConfig threadPoolsConfig;
  private SchedulerThreadPools service;

  @Override
  @Before
  public void before() throws Exception {
    super.before();
    outerExecutor = newSingleThreadExecutor();

    threadPoolsConfig = loadThreadPoolsConfig();
    threadPoolsConfig.setSchedulerPoolStrategy(DEDICATED, true);

    service = SchedulerThreadPools.builder(SchedulerThreadPoolsTestCase.class.getName(), threadPoolsConfig).build();
    service.start();
  }

  @Override
  @After
  public void after() throws Exception {
    if (service == null) {
      return;
    }
    for (Scheduler scheduler : new ArrayList<>(service.getSchedulers())) {
      scheduler.stop();
    }
    service.stop();

    outerExecutor.shutdownNow();
    outerExecutor.awaitTermination(5, SECONDS);
    super.after();
  }

  @Test
  @Description("Tests that a 'maxConcurrentTasks=1' configuration allows to execute a single task")
  public void oneConcurrentTaskSupported() throws InterruptedException {
    final ScheduledExecutorService scheduler = service
        .createIoScheduler(config().withMaxConcurrentTasks(SINGLE_TASK_THROTTLE_SIZE), SINGLE_TASK_THROTTLE_SIZE, () -> 5000L);
    Latch latch = new Latch();
    scheduler.submit(() -> latch.countDown());
    if (!latch.await(200, MILLISECONDS)) {
      fail("Task never executed");
    }
  }

  @Test
  @Description("Tests that the throttler count is consistent after task cancellation")
  public void interruptionUpdatesThrottleCounterCorrectly() throws InterruptedException, ExecutionException, TimeoutException {
    final ScheduledExecutorService scheduler = service
        .createIoScheduler(config().withMaxConcurrentTasks(SINGLE_TASK_THROTTLE_SIZE), SINGLE_TASK_THROTTLE_SIZE, () -> 5000L);

    Scheduler cpuLightScheduler = service.createCpuLightScheduler(config(), 2, () -> 5000L);

    Future<?> outerSubmit = cpuLightScheduler.submit(() -> {
      Future<?> submit = scheduler.submit(() -> {
        try {
          Thread.sleep(DEFAULT_TEST_TIMEOUT_SECS * 1000);
        } catch (InterruptedException e) {
          currentThread().interrupt();
        }
      });

      submit.cancel(true);

      CountDownLatch latch = new CountDownLatch(2);

      doSchedule(scheduler, latch);
      try {
        doSchedule(scheduler, latch);
        fail("Not rejected: " + scheduler.toString());
      } catch (RejectedExecutionException e) {
        // Expected
      }
    });

    outerSubmit.get(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);
  }

  @Test
  @Description("Tests that the throttler count is consistent after scheduled task pre-cancellation")
  public void cancellationOfScheduledUpdatesThrottleCounterCorrectly()
      throws InterruptedException, ExecutionException, TimeoutException {
    final ScheduledExecutorService scheduler = service
        .createIoScheduler(config().withMaxConcurrentTasks(SINGLE_TASK_THROTTLE_SIZE), SINGLE_TASK_THROTTLE_SIZE, () -> 5000L);

    Scheduler cpuLightScheduler = service.createCpuLightScheduler(config(), 2, () -> 5000L);

    Future<?> outerSubmit = cpuLightScheduler.submit(() -> {
      Future<?> submit = scheduler.schedule(() -> {
        // Nothing to do, this has to be cancelled before being triggered
      }, 1, HOURS);

      submit.cancel(true);

      CountDownLatch latch = new CountDownLatch(2);

      doSchedule(scheduler, latch);
      try {
        doSchedule(scheduler, latch);
        fail("Not rejected: " + scheduler.toString());
      } catch (RejectedExecutionException e) {
        // Expected
      }
    });

    outerSubmit.get(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);
  }

  @Test
  @Description("Tests that the throttler count is consistent after scheduled task cancellation")
  public void interruptionDuringExecutionOfScheduledUpdatesThrottleCounterCorrectly()
      throws InterruptedException, ExecutionException, TimeoutException {
    final ScheduledExecutorService scheduler = service
        .createIoScheduler(config().withMaxConcurrentTasks(SINGLE_TASK_THROTTLE_SIZE), SINGLE_TASK_THROTTLE_SIZE, () -> 5000L);

    Scheduler cpuLightScheduler = service.createCpuLightScheduler(config(), 2, () -> 5000L);

    Future<?> outerSubmit = cpuLightScheduler.submit(() -> {
      Future<?> submit = scheduler.schedule(() -> {
        try {
          Thread.sleep(DEFAULT_TEST_TIMEOUT_SECS * 1000);
        } catch (InterruptedException e) {
          currentThread().interrupt();
        }
      }, 10, MILLISECONDS);

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e1) {
        currentThread().interrupt();
        fail("Interrupted");
      }

      submit.cancel(true);

      CountDownLatch latch = new CountDownLatch(2);

      doSchedule(scheduler, latch);
      try {
        doSchedule(scheduler, latch);
        fail("Not rejected: " + scheduler.toString());
      } catch (RejectedExecutionException e) {
        // Expected
      }
    });

    outerSubmit.get(DEFAULT_TEST_TIMEOUT_SECS, SECONDS);
  }

  private void doSchedule(final ScheduledExecutorService scheduler, CountDownLatch latch2) {
    scheduler.submit(() -> {
      try {
        latch2.await();
      } catch (InterruptedException e) {
        currentThread().interrupt();
      }
    });
  }

  @Test
  @Description("Tests that the throttler count is decreased after scheduled task completion")
  @Issue("MULE-18909")
  public void scheduledTaskMustDecrementThrottlingCounterAfterExecution() {
    final ScheduledExecutorService scheduler = service
        .createIoScheduler(config().withMaxConcurrentTasks(SINGLE_TASK_THROTTLE_SIZE), SINGLE_TASK_THROTTLE_SIZE, () -> 5000L);

    CountDownLatch secondTaskIsExecuting = new CountDownLatch(2);

    scheduler.schedule(secondTaskIsExecuting::countDown,
                       1, MILLISECONDS);
    scheduler.schedule(secondTaskIsExecuting::countDown,
                       1000, MILLISECONDS);

    assertThat("Second task should have been executed",
               awaitLatch(secondTaskIsExecuting), is(true));
  }

  @Test
  @Description("Tests that the throttler count is decreased after scheduled task completion")
  @Issue("MULE-18909")
  public void scheduledTaskMustDecrementThrottlingCounterAfterExecutionNested() {
    final ScheduledExecutorService scheduler = service
        .createIoScheduler(config().withMaxConcurrentTasks(SINGLE_TASK_THROTTLE_SIZE), SINGLE_TASK_THROTTLE_SIZE, () -> 5000L);

    CountDownLatch secondTaskIsExecuting = new CountDownLatch(1);

    scheduler.schedule(() -> scheduler.schedule(secondTaskIsExecuting::countDown,
                                                1000, MILLISECONDS),
                       1000, MILLISECONDS);

    assertThat("Second task should have been executed",
               awaitLatch(secondTaskIsExecuting), is(true));
  }

  @Test
  @Description("Tests that a task submitted in excess of 'maxConcurrentTasks' waits until another task finishes before executing.")
  public void throttledTask() throws InterruptedException {
    final ScheduledExecutorService scheduler =
        service.createIoScheduler(config().withMaxConcurrentTasks(THROTTLE_SIZE), THROTTLE_SIZE, () -> 5000L);
    final Latch latch = new Latch();

    for (int i = 0; i < THROTTLE_SIZE; ++i) {
      scheduler.execute(() -> {
        awaitLatch(latch);
      });
    }

    final Future<?> throttledSubmission = outerExecutor.submit(() -> {
      scheduler.execute(() -> {
        // Nothing to do
      });
    });

    sleep(10);
    assertThat(throttledSubmission.isDone(), is(false));

    latch.countDown();

    new PollingProber(100, 10).check(new JUnitLambdaProbe(() -> {
      assertThat(throttledSubmission.isDone(), is(true));
      return true;
    }));
  }

  @Test
  @Description("Tests that a task submitted in excess of 'maxConcurrentTasks' is rejected when called from a cpu-processing thread.")
  public void throttledTaskRejected() throws InterruptedException, ExecutionException, TimeoutException {
    final ScheduledExecutorService scheduler = service
        .createIoScheduler(config().withMaxConcurrentTasks(SINGLE_TASK_THROTTLE_SIZE), SINGLE_TASK_THROTTLE_SIZE, () -> 5000L);
    final Latch latch = new Latch();

    for (int i = 0; i < THROTTLE_SIZE; ++i) {
      scheduler.execute(() -> {
        awaitLatch(latch);
      });
    }

    Scheduler cpuLightScheduler = service.createCpuLightScheduler(config(), 2, () -> 5000L);

    Future<?> submittedTest = cpuLightScheduler.submit(() -> {
      try {
        scheduler.execute(() -> {
          // Nothing to do
        });
        fail("Expected the task to be rejected with a 'SchedulerTaskThrottledException'");
      } catch (SchedulerTaskThrottledException rejected) {
        // Nothing to do
      }
    });

    submittedTest.get(1, SECONDS);
  }

  @Test
  @Description("A deadlock does not happen when dispatching max+2 tasks to a throttled scheduler")
  @Issue("MULE-17938")
  public void maxPlusTwoNoDeadlockWaitGroup() throws InterruptedException, ExecutionException, TimeoutException {
    final ScheduledExecutorService scheduler = service
        .createIoScheduler(config().withMaxConcurrentTasks(SINGLE_TASK_THROTTLE_SIZE), SINGLE_TASK_THROTTLE_SIZE,
                           () -> 5000L);

    final int totalTasks = SINGLE_TASK_THROTTLE_SIZE + 2;
    Scheduler waitAllowed = service.createIoScheduler(config(), totalTasks, () -> 5000L);

    final Latch innerLatch = new Latch();

    final List<Future> tasks = new ArrayList<>();

    for (int i = 0; i < totalTasks; ++i) {
      waitAllowed.execute(() -> {
        tasks.add(scheduler.submit(() -> {
          return awaitLatch(innerLatch);
        }));
      });
    }

    sleep(1000);
    innerLatch.countDown();

    probe(() -> {
      assertThat(tasks, hasSize(totalTasks));
      for (Future task : tasks) {
        assertThat(task.get(1, SECONDS), is(true));
      }
      return true;
    });
  }

  @Test
  @Description("A deadlock does not happen when dispatching max+2 tasks to a throttled scheduler")
  @Issue("MULE-17938")
  public void maxPlusTwoNoDeadlockNotWaitGroup() throws InterruptedException, ExecutionException, TimeoutException {
    final ScheduledExecutorService scheduler = service
        .createIoScheduler(config().withMaxConcurrentTasks(SINGLE_TASK_THROTTLE_SIZE), SINGLE_TASK_THROTTLE_SIZE,
                           () -> 5000L);

    final int totalTasks = SINGLE_TASK_THROTTLE_SIZE + 2;
    Scheduler notWaitAllowed = service.createCpuLightScheduler(config(), totalTasks, () -> 5000L);

    final Latch innerLatch = new Latch();

    final List<Future> tasks = new ArrayList<>();

    for (int i = 0; i < totalTasks; ++i) {
      notWaitAllowed.execute(() -> {
        tasks.add(scheduler.submit(() -> {
          return awaitLatch(innerLatch);
        }));
      });
    }

    sleep(1000);
    innerLatch.countDown();

    probe(() -> {
      assertThat(tasks, hasSize(1));
      for (Future task : tasks) {
        assertThat(task.get(1, SECONDS), is(true));
      }
      return true;
    });

    notWaitAllowed.submit(() -> {
      tasks.add(scheduler.submit(() -> {
        return true;
      }));
    }).get(1, SECONDS);
  }

  @Test
  @Description("A throttled scheduler may accept many scheduled tasks and throttle then when they actually execute.")
  @Issue("MULE-18053")
  public void scheduleOnThrottledScheduler() throws InterruptedException, ExecutionException, TimeoutException {
    final ScheduledExecutorService scheduler = service
        .createIoScheduler(config().withMaxConcurrentTasks(SINGLE_TASK_THROTTLE_SIZE), SINGLE_TASK_THROTTLE_SIZE,
                           () -> 5000L);

    final int totalTasks = 2;
    Scheduler waitAllowed = service.createIoScheduler(config(), totalTasks, () -> 5000L);

    final Latch innerLatch = new Latch();

    final List<Future> tasks = new ArrayList<>();

    for (int i = 0; i < totalTasks; ++i) {
      waitAllowed.execute(() -> {
        tasks.add(scheduler.schedule(() -> {
          return awaitLatch(innerLatch);
        }, 1, SECONDS));
      });
    }

    probe(() -> {
      assertThat(tasks, hasSize(totalTasks));
      return true;
    });
  }
}
