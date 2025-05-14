/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import org.mule.runtime.api.exception.MuleRuntimeException;

import java.util.concurrent.Delayed;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;

public class QuartzScheduledFuture<V> extends AbstractDelegatedScheduledFuture<V> {

  private final Scheduler quartzScheduler;
  private final Trigger trigger;

  /**
   *
   * @param task the actual task that was scheduled.
   */
  QuartzScheduledFuture(org.quartz.Scheduler quartzScheduler, Trigger trigger, RunnableFuture<?> task) {
    super((RunnableFuture<V>) task);
    this.quartzScheduler = quartzScheduler;
    this.trigger = trigger;
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
  public boolean isDone() {
    return task.isCancelled() || task.isDone();
  }

}
