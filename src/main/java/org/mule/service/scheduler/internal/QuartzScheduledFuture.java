/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import org.mule.runtime.api.exception.MuleRuntimeException;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;

public class QuartzScheduledFuture<V> implements ScheduledFuture<V> {

  private final Scheduler quartzScheduler;
  private final Trigger trigger;
  private final RunnableFuture<?> task;

  /**
   *
   * @param task the actual task that was scheduled.
   */
  QuartzScheduledFuture(org.quartz.Scheduler quartzScheduler, Trigger trigger, RunnableFuture<?> task) {
    this.quartzScheduler = quartzScheduler;
    this.trigger = trigger;
    this.task = task;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(trigger.getNextFireTime().toInstant().getNano() - nanoTime(), NANOSECONDS);
  }

  @Override
  public int compareTo(Delayed o) {
    long diff = getDelay(NANOSECONDS) - o.getDelay(NANOSECONDS);

    if (diff < 0) {
      return -1;
    } else if (diff > 0) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    boolean quartzCancelled;
    try {
      quartzCancelled = quartzScheduler.unscheduleJob(trigger.getKey());
    } catch (SchedulerException e) {
      throw new MuleRuntimeException(e);
    }
    boolean taskCancelled = task.cancel(mayInterruptIfRunning);
    return quartzCancelled || taskCancelled;
  }

  @Override
  public boolean isCancelled() {
    return task.isCancelled();
  }

  @Override
  public boolean isDone() {
    return task.isCancelled() || task.isDone();
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    return (V) task.get();
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return (V) task.get(timeout, unit);
  }

}
