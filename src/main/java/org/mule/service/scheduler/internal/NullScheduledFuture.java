/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal;

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * No-op implementation of a {@link ScheduledFuture}.
 *
 * @since 1.0
 */
final class NullScheduledFuture<V> implements ScheduledFuture<V> {

  public static final NullScheduledFuture<?> INSTANCE = new NullScheduledFuture<>();

  private NullScheduledFuture() {
    // Nothing to do
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return 0;
  }

  @Override
  public int compareTo(Delayed o) {
    return 0;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return false;
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    return null;
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return null;
  }
}
