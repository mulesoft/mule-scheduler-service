/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal;

import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SchedulerServiceStory.TASK_SCHEDULING;

import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.mockito.InOrder;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;

@Feature(SCHEDULER_SERVICE)
@Story(TASK_SCHEDULING)
@RunWith(Parameterized.class)
public class DefaultSchedulerFixedDelayRepeatsTestCase extends BaseDefaultSchedulerTestCase {

  private static final long TASK_DURATION_MILLIS = 200;
  private static final long TEST_DELAY_MILLIS = 1000;

  protected ScheduledExecutorService executor;

  public DefaultSchedulerFixedDelayRepeatsTestCase(BlockingQueue<Runnable> sharedExecutorQueue, String param) {
    this.sharedExecutorQueue = sharedExecutorQueue;
  }

  @Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return asList(new Object[][] {
        {new SynchronousQueue<>(), "syncQueue"},
        {new LinkedBlockingQueue<>(1), "queue(1)"}
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

  @Test
  @Description("Tests that scheduleAtFixedDelay parameters are honored")
  public void fixedDelayRepeats() {
    List<Long> startTimes = new ArrayList<>();
    List<Long> endTimes = new ArrayList<>();

    final CountDownLatch latch = new CountDownLatch(2);

    final ScheduledFuture<?> scheduled = executor.scheduleWithFixedDelay(() -> {
      startTimes.add(nanoTime());
      try {
        sleep(TASK_DURATION_MILLIS);
      } catch (InterruptedException e) {
        currentThread().interrupt();
      }
      latch.countDown();
      endTimes.add(nanoTime());
    }, 0, TEST_DELAY_MILLIS, MILLISECONDS);

    assertThat(awaitLatch(latch), is(true));
    scheduled.cancel(true);

    InOrder inOrder = inOrder(sharedScheduledExecutor);
    inOrder.verify(sharedScheduledExecutor).schedule(any(Runnable.class), eq(0L), eq(MILLISECONDS));
    inOrder.verify(sharedScheduledExecutor).schedule(any(Runnable.class), eq(TEST_DELAY_MILLIS), eq(MILLISECONDS));
    assertThat(NANOSECONDS.toMillis(startTimes.get(1) - endTimes.get(0)),
               greaterThanOrEqualTo(TEST_DELAY_MILLIS - DELTA_MILLIS));
  }

}
