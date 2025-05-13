/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of {@link ScheduledFuture} for tasks that have no pending delay. This can represent tasks that are intended for
 * immediate execution or tasks that have already been cancelled prior to scheduling. All operations are delegated to the
 * underlying {@link RunnableFuture}.
 *
 * @since 1.10
 */
final class ZeroDelayScheduledFuture<V> implements ScheduledFuture<V> {

  private final RunnableFuture<V> task;

  /**
   * Creates a new instance that delegates to the given task.
   *
   * @param task the task to delegate to
   */
  ZeroDelayScheduledFuture(RunnableFuture<V> task) {
    this.task = task;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return 0; // executed immediately
  }

  @Override
  public int compareTo(Delayed o) {
    if (o == this)
      return 0;
    long diff = getDelay(TimeUnit.NANOSECONDS) - o.getDelay(TimeUnit.NANOSECONDS);
    return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return task.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return task.isCancelled();
  }

  @Override
  public boolean isDone() {
    return task.isDone();
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    return task.get();
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return task.get(timeout, unit);
  }
}
