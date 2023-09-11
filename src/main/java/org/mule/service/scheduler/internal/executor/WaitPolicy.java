/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.executor;

import static java.lang.Long.MAX_VALUE;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerBusyException;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A handler for unexecutable tasks that waits until the task can be submitted for execution or times out. Generously snipped from
 * the jsr166 repository at:
 * <a href="http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/main/java/util/concurrent/ThreadPoolExecutor.java"></a>.
 */
public class WaitPolicy implements RejectedExecutionHandler {

  private final long time;
  private final TimeUnit timeUnit;

  private final RejectedExecutionHandler shutdownPolicy;

  private final String schedulerName;

  /**
   * Constructs a <tt>WaitPolicy</tt> which waits (almost) forever.
   *
   * @param shutdownPolicy the policy to use when the executor is shutdown.
   * @param scheduelrName  the name of the target {@link Scheduler}
   */
  public WaitPolicy(RejectedExecutionHandler shutdownPolicy, String schedulerName) {
    // effectively waits forever
    this(MAX_VALUE, SECONDS, shutdownPolicy, schedulerName);
  }

  /**
   * Constructs a <tt>WaitPolicy</tt> with timeout. A negative <code>time</code> value is interpreted as
   * <code>Long.MAX_VALUE</code>.
   *
   * @param shutdownPolicy the policy to use when the executor is shutdown.
   * @param schedulerName  the name of the target {@link Scheduler}
   */
  public WaitPolicy(long time, TimeUnit timeUnit, RejectedExecutionHandler shutdownPolicy, String schedulerName) {
    super();
    this.time = (time < 0 ? MAX_VALUE : time);
    this.timeUnit = timeUnit;
    this.shutdownPolicy = shutdownPolicy;
    this.schedulerName = schedulerName;
  }

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
    if (e.isShutdown()) {
      shutdownPolicy.rejectedExecution(r, e);
    } else {
      try {
        if (!e.getQueue().offer(r, time, timeUnit)) {
          throw new SchedulerBusyException(format("Scheduler '%s' did not accept within %1d %2s", schedulerName, time, timeUnit));
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RejectedExecutionException(ie);
      }
    }
  }


}
