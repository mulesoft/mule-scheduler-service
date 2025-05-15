/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Decorates a {@link ScheduledFuture} in order to propagate the calls to {@link Future} methods not only to the decorated task
 * but also to the {@link RunnableFuture} that represents the scheduled task.
 *
 * @since 1.0
 */
class ScheduledFutureDecorator<V> implements ScheduledFuture<V> {

  private ScheduledFuture<V> scheduled;
  private RunnableFuture<?> task;
  private boolean periodic;

  /**
   * Decorates the given {@code scheduled}
   *
   * @param scheduled the {@link ScheduledFuture} to be decorated
   * @param task      the actual task that was scheduled.
   * @param periodic  {@code true} if this task will run on a regular basis, {@code false} otherwise
   */
  ScheduledFutureDecorator(ScheduledFuture<V> scheduled, RunnableFuture<?> task, boolean periodic) {
    this.scheduled = scheduled;
    this.task = task;
    this.periodic = periodic;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return scheduled.getDelay(unit);
  }

  @Override
  public int compareTo(Delayed o) {
    return scheduled.compareTo(o);
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    boolean scheduledCancelled = scheduled.cancel(mayInterruptIfRunning);
    boolean taskCancelled = task.cancel(mayInterruptIfRunning);
    return scheduledCancelled || taskCancelled;
  }

  @Override
  public boolean isCancelled() {
    return scheduled.isCancelled() || task.isCancelled();
  }

  @Override
  public boolean isDone() {
    return scheduled.isDone() || task.isDone();
  }

  /**
   * @return {@code true} if this task will run on a regular basis, {@code false} otherwise
   */
  public boolean isPeriodic() {
    return periodic;
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
