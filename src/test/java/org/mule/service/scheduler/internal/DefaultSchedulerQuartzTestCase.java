/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.TimeZone.getDefault;
import static java.util.TimeZone.getTimeZone;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mule.service.scheduler.ThreadType.CUSTOM;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SchedulerServiceStory.QUARTZ_TASK_SCHEDULING;
import org.mule.tck.probe.JUnitLambdaProbe;
import org.mule.tck.probe.PollingProber;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

@Feature(SCHEDULER_SERVICE)
@Story(QUARTZ_TASK_SCHEDULING)
public class DefaultSchedulerQuartzTestCase extends BaseDefaultSchedulerTestCase {

  private static final int DELTA_MILLIS = 30;

  private DefaultScheduler executor;

  @Override
  public void before() throws Exception {
    super.before();
    executor = new DefaultScheduler(DefaultSchedulerQuartzTestCase.class.getSimpleName(), sharedExecutor, 1,
                                    sharedScheduledExecutor, sharedQuartzScheduler, CUSTOM, () -> 5000L,
                                    EMPTY_SHUTDOWN_CALLBACK, null);
  }

  @Override
  public void after() throws Exception {
    executor.shutdownNow();
    executor.awaitTermination(5, SECONDS);
    super.after();
  }

  @Test
  @Description("Tests that a ScheduledFuture from a cron is properly cancelled before it starts executing")
  public void cancelCronBeforeFire() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.scheduleWithCronExpression(() -> {
      awaitLatch(latch);
    }, "* * * ? * * 2099");

    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  @Test
  @Description("Tests that a ScheduledFuture from a cron is properly cancelled while it's executing")
  public void cancelCronWhileRunning() throws InterruptedException {
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.scheduleWithCronExpression(() -> {
      latch1.countDown();
      awaitLatch(latch2);
    }, "1/10 * * ? * *");

    latch1.await();
    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  @Test
  @Description("Tests that a ScheduledFuture from a cron is properly cancelled in-between executions")
  public void cancelCronInBetweenRuns() throws InterruptedException, ExecutionException {
    final CountDownLatch latch = new CountDownLatch(1);

    final ScheduledFuture<?> scheduled = executor.scheduleWithCronExpression(() -> {
      sharedScheduledExecutor.schedule(() -> latch.countDown(), 0, SECONDS);
    }, "0/30 * * ? * *");

    latch.await();
    scheduled.cancel(true);

    assertCancelled(scheduled);
    assertTerminationIsNotDelayed(executor);
  }

  private void assertCancelled(final ScheduledFuture<?> scheduled) {
    assertThat(scheduled.isCancelled(), is(true));
    assertThat(scheduled.isDone(), is(true));
  }

  @Override
  protected void assertTerminationIsNotDelayed(final ScheduledExecutorService executor) throws InterruptedException {
    long startTime = nanoTime();
    executor.shutdown();
    executor.awaitTermination(1000, MILLISECONDS);

    assertThat((double) NANOSECONDS.toMillis(nanoTime() - startTime), closeTo(0, DELTA_MILLIS));
  }

  @Test
  @Description("Tests that cron schedule parameters are honored")
  public void cronRepeats() throws SchedulerException {
    List<Long> startTimes = new ArrayList<>();
    List<Long> endTimes = new ArrayList<>();

    final CountDownLatch latch = new CountDownLatch(3);

    final String everyTwoSeconds = "0/2 * * ? * *";
    final ScheduledFuture<?> scheduled = executor.scheduleWithCronExpression(() -> {
      startTimes.add(System.nanoTime());
      try {
        sleep(200);
      } catch (InterruptedException e) {
        currentThread().interrupt();
      }
      latch.countDown();
      endTimes.add(System.nanoTime());
    }, everyTwoSeconds);

    assertThat(awaitLatch(latch), is(true));
    scheduled.cancel(true);

    verify(sharedQuartzScheduler).scheduleJob(any(JobDetail.class), argThat(new CronTriggerMatcher(everyTwoSeconds)));
  }

  @Test
  @Description("Tests that cron schedule parameters are honored even if the task takes longer than the interval")
  public void cronExceeds() throws SchedulerException {
    List<Long> startTimes = new ArrayList<>();
    List<Long> endTimes = new ArrayList<>();

    final CountDownLatch latch = new CountDownLatch(3);

    final String everySecond = "0/1 * * ? * *";
    final ScheduledFuture<?> scheduled = executor.scheduleWithCronExpression(() -> {
      startTimes.add(System.nanoTime());
      latch.countDown();

      try {
        sleep(1200);
      } catch (InterruptedException e) {
        currentThread().interrupt();
      }

      endTimes.add(System.nanoTime());
    }, everySecond);

    assertThat(awaitLatch(latch), is(true));
    scheduled.cancel(true);

    verify(sharedQuartzScheduler).scheduleJob(any(JobDetail.class), argThat(new CronTriggerMatcher(everySecond)));

    new PollingProber().check(new JUnitLambdaProbe(() -> {
      assertThat(startTimes.size(), is(3));
      assertThat(endTimes.size(), is(3));
      assertThat((double) NANOSECONDS.toMillis(startTimes.get(2) - endTimes.get(1)), closeTo(0, DELTA_MILLIS));
      assertThat((double) NANOSECONDS.toMillis(startTimes.get(1) - endTimes.get(0)), closeTo(0, DELTA_MILLIS));
      return true;
    }));
  }

  @Test
  @Description("Tests that when scheduling with cron with no timezone, the default is passed on to quartz")
  public void cronWithDefaultTimezone() {
    executor.setJobClass(StoresTimeZoneJob.class);

    executor.scheduleWithCronExpression(() -> {
    }, "0/1 * * ? * *");

    new PollingProber().check(new JUnitLambdaProbe(() -> {
      assertThat(StoresTimeZoneJob.getTimeZone(), is(getDefault()));
      return true;
    }));
  }

  @Test
  @Description("Tests that the timezone when scheduling with cron is passed on to quartz")
  public void cronWithCustom1Timezone() {
    executor.setJobClass(StoresTimeZoneJob.class);

    TimeZone timeZone = getTimeZone("America/Argentina/Buenos_Aires");
    executor.scheduleWithCronExpression(() -> {
    }, "0/1 * * ? * *", timeZone);

    new PollingProber().check(new JUnitLambdaProbe(() -> {
      assertThat(StoresTimeZoneJob.getTimeZone(), is(timeZone));
      return true;
    }));
  }

  @Test
  @Description("Tests that the timezone when scheduling with cron is passed on to quartz. "
      + "This test is needed in case that the previous test's timezone is the same as the default.")
  public void cronWithCustom2Timezone() {
    executor.setJobClass(StoresTimeZoneJob.class);

    TimeZone timeZone = getTimeZone("Europe/London");
    executor.scheduleWithCronExpression(() -> {
    }, "0/1 * * ? * *", timeZone);

    new PollingProber().check(new JUnitLambdaProbe(() -> {
      assertThat(StoresTimeZoneJob.getTimeZone(), is(timeZone));
      return true;
    }));
  }

  private static final class CronTriggerMatcher implements ArgumentMatcher<CronTrigger> {

    final String cronExpression;// = "0/2 * * ? * *";
    final TimeZone timeZone;

    public CronTriggerMatcher(String cronExpression) {
      this(cronExpression, getDefault());
    }

    public CronTriggerMatcher(String cronExpression, TimeZone timeZone) {
      this.cronExpression = cronExpression;
      this.timeZone = timeZone;
    }

    @Override
    public boolean matches(CronTrigger item) {
      return item.getCronExpression().equals(cronExpression) && item.getTimeZone().equals(timeZone);
    }
  }

  public static class StoresTimeZoneJob extends QuartzCronJob implements Job {

    private static TimeZone timeZone;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      timeZone = ((CronTrigger) (context.getTrigger())).getTimeZone();
      super.execute(context);
    }

    public static TimeZone getTimeZone() {
      return timeZone;
    }
  }
}
