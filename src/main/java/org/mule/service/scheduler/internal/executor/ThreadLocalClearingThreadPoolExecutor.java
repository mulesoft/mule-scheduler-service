/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.executor;

import org.mule.runtime.api.exception.MuleRuntimeException;

import java.lang.reflect.Field;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.currentThread;

/**
 * A special {@link ThreadPoolExecutor} which clears all thread-locals after each task is executed and before returning the thread
 * to the pool.
 */
public class ThreadLocalClearingThreadPoolExecutor extends ThreadPoolExecutor {

  /**
   * Reference to the threadLocals field of the {@link Thread} class, this is necessary because it is not accessible by default.
   */
  private static final Field threadLocalsField;

  static {
    try {
      // Grabs a reference to the threadLocals field and sets it as accessible using reflection.
      threadLocalsField = Thread.class.getDeclaredField("threadLocals");
      threadLocalsField.setAccessible(true);
    } catch (NoSuchFieldException | SecurityException e) {
      throw new RuntimeException(e);
    }
  }

  public ThreadLocalClearingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                               BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
                                               RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    clearAllThreadLocals();
  }

  /**
   * Effectively clears all thread-local variables on the current thread.
   */
  protected static void clearAllThreadLocals() {
    try {
      threadLocalsField.set(currentThread(), null);
    } catch (Exception e) {
      throw new MuleRuntimeException(e);
    }
  }
}
