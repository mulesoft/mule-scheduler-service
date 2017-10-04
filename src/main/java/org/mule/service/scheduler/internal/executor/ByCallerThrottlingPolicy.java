/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.executor;

import static java.lang.Thread.currentThread;
import static org.apache.commons.lang3.StringUtils.rightPad;
import static org.mule.service.scheduler.internal.DefaultSchedulerService.USAGE_TRACE_INTERVAL_SECS;
import static org.mule.service.scheduler.internal.DefaultSchedulerService.traceLogger;

import org.mule.runtime.api.exception.MuleRuntimeException;
import org.mule.runtime.api.scheduler.SchedulerService;
import org.mule.service.scheduler.internal.ThrottledScheduler;

import java.util.Set;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dynamically determines how to handle a task dispatch when its throttling max value has been reached for a
 * {@link ThrottledScheduler}.
 * <p>
 * Each {@link ThrottledScheduler} must have its own instance of a {@link ByCallerThrottlingPolicy} since it keeps state relative
 * to the owning {@link ThrottledScheduler}.
 *
 * @since 1.0
 */
public final class ByCallerThrottlingPolicy extends AbstractByCallerPolicy {

  private final int maxConcurrentTasks;
  private final AtomicInteger runningTasks = new AtomicInteger();

  private volatile long rejectedCount;

  /**
   * Builds a new {@link ByCallerThrottlingPolicy} with the given {@code waitGroups}.
   *
   * @param maxConcurrentTasks how many tasks can be running at the same time for the owner {@link ThrottledScheduler}.
   * @param waitGroups the group of threads for which waiting will be applied. For the rest, any task exceeding the throttle value
   *        will be rejected.
   * @param parentGroup the {@link SchedulerService} parent {@link ThreadGroup}
   */
  public ByCallerThrottlingPolicy(int maxConcurrentTasks, Set<ThreadGroup> waitGroups, ThreadGroup parentGroup) {
    super(waitGroups, parentGroup);
    this.maxConcurrentTasks = maxConcurrentTasks;
  }

  public void throttle(Runnable throttledCallback, RunnableFuture<?> task, String schedulerRepresentation) {
    ThreadGroup currentThreadGroup = currentThread().getThreadGroup();

    ++rejectedCount;

    if (!isSchedulerThread(currentThreadGroup) || isWaitGroupThread(currentThreadGroup)) {
      try {
        synchronized (runningTasks) {
          runningTasks.incrementAndGet();
          while (runningTasks.get() > maxConcurrentTasks) {
            logThrottle(task.toString(), "WaitPolicy", schedulerRepresentation);
            runningTasks.wait();
          }
        }
        throttledCallback.run();
      } catch (InterruptedException e) {
        currentThread().interrupt();
        throw new MuleRuntimeException(e);
      }
    } else {
      synchronized (runningTasks) {
        if (runningTasks.incrementAndGet() > maxConcurrentTasks) {
          logThrottle(task.toString(), "AbortPolicy", schedulerRepresentation);
          throw new SchedulerTaskThrottledException("Task '" + task.toString() + "' throttled back from '"
              + schedulerRepresentation + "'");
        } else {
          throttledCallback.run();
        }
      }
    }
  }

  public void throttleWrapUp() {
    synchronized (runningTasks) {
      runningTasks.decrementAndGet();
      runningTasks.notify();
    }
  }

  private void logThrottle(String taskAsString, String strategy, String targetAsString) {
    if (USAGE_TRACE_INTERVAL_SECS != null) {
      traceLogger.warn("Task throttled back ({}) from '{}' scheduler: {}", rightPad(strategy, 16), targetAsString, taskAsString);
    } else if (traceLogger.isDebugEnabled()) {
      traceLogger.debug("Task throttled back ({}) from '{}' scheduler: {}", rightPad(strategy, 16), targetAsString, taskAsString);
    }
  }

  public long getRejectedCount() {
    return rejectedCount;
  }

  @Override
  public String toString() {
    return "(throttling: " + runningTasks.get() + "/" + maxConcurrentTasks + ")";
  }
}
