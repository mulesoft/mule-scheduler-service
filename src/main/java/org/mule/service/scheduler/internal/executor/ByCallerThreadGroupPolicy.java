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

import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerBusyException;
import org.mule.runtime.api.scheduler.SchedulerService;
import org.mule.service.scheduler.internal.threads.SchedulerThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;

/**
 * Dynamically determines the {@link RejectedExecutionHandler} implementation to use according to the {@link ThreadGroup} of the
 * current thread. If the current thread is not a {@link SchedulerService} managed thread then
 * {@link WaitPolicy} is used.
 *
 * @see AbortPolicy
 * @see WaitPolicy
 *
 * @since 1.0
 */
public final class ByCallerThreadGroupPolicy extends AbstractByCallerPolicy implements RejectedExecutionHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ByCallerThreadGroupPolicy.class);

  private static final class AbortBusyPolicy extends AbortPolicy {

    private final String schedulerName;

    public AbortBusyPolicy(String schedulerName) {
      this.schedulerName = schedulerName;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      String msg = "Task '" + r.toString() + "' rejected from Scheduler '" + schedulerName + "' ('"
          + executor.toString() + "')";
      LOGGER.warn(msg);
      throw new SchedulerBusyException(msg);
    }
  }

  private final ThreadGroup couLightGroup;

  private final AbortPolicy abort;
  private final WaitPolicy wait;
  private final CallerRunsPolicy callerRuns = new CallerRunsPolicy();

  private volatile long rejectedCount;


  /**
   * Builds a new {@link ByCallerThreadGroupPolicy} with the given {@code waitGroups}.
   *
   * @param waitGroups the group of threads for which a {@link WaitPolicy} will be applied. For the rest, an {@link AbortPolicy}
   *        (or {@link CallerRunsPolicy} if allowed) will be applied.
   * @param runCpuLightWhenTargetBusyGroups the group of threads for which a {@link CallerRunsPolicy} will be applied when
   *        dispatching to cpu-light.
   * @param couLightGroup the group of cpuLight threads
   * @param parentGroup the {@link SchedulerService} parent {@link ThreadGroup}
   * @param schedulerName the name of the target {@link Scheduler}
   */
  public ByCallerThreadGroupPolicy(Set<ThreadGroup> waitGroups, Set<ThreadGroup> runCpuLightWhenTargetBusyGroups,
                                   ThreadGroup couLightGroup, ThreadGroup parentGroup, String schedulerName) {
    super(waitGroups, runCpuLightWhenTargetBusyGroups, parentGroup);
    this.abort = new AbortBusyPolicy(schedulerName);
    this.wait = new WaitPolicy(abort, schedulerName);
    this.couLightGroup = couLightGroup;
  }

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    ThreadGroup targetGroup = ((SchedulerThreadFactory) executor.getThreadFactory()).getGroup();
    ThreadGroup currentThreadGroup = currentThread().getThreadGroup();

    ++rejectedCount;

    if ((isRunCpuLightWhenTargetBusyThread(currentThreadGroup) && targetGroup == couLightGroup)
        || (isWaitGroupThread(targetGroup) && targetGroup == currentThreadGroup)) {
      if (isLogRejectionEnabled()) {
        logRejection(r.toString(), callerRuns.getClass().getSimpleName(), targetGroup.getName());
      }
      callerRuns.rejectedExecution(r, executor);
    } else if (!isSchedulerThread(currentThreadGroup) || isWaitGroupThread(currentThreadGroup)) {
      if (isLogRejectionEnabled()) {
        logRejection(r.toString(), wait.getClass().getSimpleName(), targetGroup.getName());
      }
      // MULE-11460 Make CPU-intensive pool a ForkJoinPool - keep the parallelism when waiting.
      wait.rejectedExecution(r, executor);
    } else {
      if (isLogRejectionEnabled()) {
        logRejection(r.toString(), abort.getClass().getSimpleName(), targetGroup.getName());
      }
      abort.rejectedExecution(r, executor);
    }
  }

  private boolean isLogRejectionEnabled() {
    return USAGE_TRACE_INTERVAL_SECS != null ? traceLogger.isWarnEnabled() : traceLogger.isDebugEnabled();
  }

  private void logRejection(String taskAsString, String strategy, String targetAsString) {
    if (USAGE_TRACE_INTERVAL_SECS != null) {
      traceLogger.warn("Task rejected ({}) from '{}' scheduler: {}", rightPad(strategy, 16), targetAsString, taskAsString);
    } else {
      traceLogger.debug("Task rejected ({}) from '{}' scheduler: {}", rightPad(strategy, 16), targetAsString, taskAsString);
    }
  }

  public long getRejectedCount() {
    return rejectedCount;
  }

}
