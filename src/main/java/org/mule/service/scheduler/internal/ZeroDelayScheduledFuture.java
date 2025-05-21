/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.Delayed;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link ScheduledFuture} for tasks that have no pending delay. This can represent tasks that are intended for
 * immediate execution or tasks that have already been cancelled prior to scheduling. All operations are delegated to the
 * underlying {@link RunnableFuture}.
 *
 * @since 1.10
 */
final class ZeroDelayScheduledFuture<V> extends AbstractDelegatedScheduledFuture<V> {

  /**
   * Creates a new instance that delegates to the given task.
   *
   * @param task the task to delegate to
   */
  ZeroDelayScheduledFuture(RunnableFuture<V> task) {
    super(task);
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return 0;
  }

  @Override
  public int compareTo(Delayed o) {
    long diff = getDelay(NANOSECONDS) - o.getDelay(NANOSECONDS);
    return Long.compare(diff, 0);
  }
}
