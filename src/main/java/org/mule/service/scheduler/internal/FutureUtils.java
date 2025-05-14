/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utility methods for Future implementations that delegate operations to an underlying Future.
 *
 * @since 1.10
 */
public final class FutureUtils {

  private FutureUtils() {}

  /**
   * Delegates the isDone operation to the provided Future.
   *
   * @param delegate the Future to delegate to
   * @return the result of isDone on the delegate
   */
  public static boolean isDone(Future<?> delegate) {
    return delegate.isDone();
  }

  /**
   * Delegates the isCancelled operation to the provided Future.
   *
   * @param delegate the Future to delegate to
   * @return the result of isCancelled on the delegate
   */
  public static boolean isCancelled(Future<?> delegate) {
    return delegate.isCancelled();
  }

  /**
   * Delegates the get operation to the provided Future.
   *
   * @param delegate the Future to delegate to
   * @return the result of get on the delegate
   * @throws InterruptedException if the delegate throws InterruptedException
   * @throws ExecutionException   if the delegate throws ExecutionException
   */
  public static <V> V get(Future<V> delegate) throws InterruptedException, ExecutionException {
    return delegate.get();
  }

  /**
   * Delegates the get operation with timeout to the provided Future.
   *
   * @param delegate the Future to delegate to
   * @param timeout  the maximum time to wait
   * @param unit     the time unit of the timeout argument
   * @return the result of get on the delegate
   * @throws InterruptedException if the delegate throws InterruptedException
   * @throws ExecutionException   if the delegate throws ExecutionException
   * @throws TimeoutException     if the delegate throws TimeoutException
   */
  public static <V> V get(Future<V> delegate, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return delegate.get(timeout, unit);
  }
}
