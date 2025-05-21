/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Abstract base class for {@link ScheduledFuture} implementations that delegate most operations to an underlying
 * {@link RunnableFuture}.
 *
 * @since 1.10
 */
abstract class AbstractDelegatedScheduledFuture<V> implements ScheduledFuture<V> {

  protected final RunnableFuture<V> task;

  /**
   * Creates a new instance that delegates to the given task.
   *
   * @param task the task to delegate to
   */
  protected AbstractDelegatedScheduledFuture(RunnableFuture<V> task) {
    this.task = task;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return task.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return FutureUtils.isCancelled(task);
  }

  @Override
  public boolean isDone() {
    return FutureUtils.isDone(task);
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    return FutureUtils.get(task);
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return FutureUtils.get(task, timeout, unit);
  }
}
