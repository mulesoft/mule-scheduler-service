/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.executor;

import static java.lang.Thread.currentThread;
import static java.util.Collections.unmodifiableSet;
import static org.apache.commons.lang3.StringUtils.rightPad;
import static org.slf4j.LoggerFactory.getLogger;

import org.mule.runtime.core.api.scheduler.SchedulerBusyException;
import org.mule.service.scheduler.internal.threads.SchedulerThreadFactory;

import java.util.Set;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;

import org.slf4j.Logger;

/**
 * Dynamically determines the {@link RejectedExecutionHandler} implementation to use according to the {@link ThreadGroup} of the
 * current thread. If the current thread is not a {@link org.mule.runtime.core.api.scheduler.SchedulerService} managed thread then
 * {@link WaitPolicy} is used.
 * 
 * @see AbortPolicy
 * @see WaitPolicy
 * 
 * @since 1.0
 */
public final class ByCallerThreadGroupPolicy implements RejectedExecutionHandler {

  private static final Logger LOGGER = getLogger(ByCallerThreadGroupPolicy.class);

  private final AbortPolicy abort = new AbortPolicy() {

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      throw new SchedulerBusyException("Task " + r.toString() + " rejected from " + executor.toString());
    }
  };
  private final WaitPolicy wait = new WaitPolicy();
  private final CallerRunsPolicy callerRuns = new CallerRunsPolicy();

  private final Set<ThreadGroup> waitGroups;
  private final ThreadGroup parentGroup;

  private final boolean logRejectionDetails;

  /**
   * Builds a new {@link ByCallerThreadGroupPolicy} with the given {@code waitGroups}.
   * 
   * @param waitGroups the group of threads for which a {@link WaitPolicy} will be applied. For the rest, an {@link AbortPolicy}
   *        will be applied.
   * @param parentGroup the {@link org.mule.runtime.core.api.scheduler.SchedulerService} parent {@link ThreadGroup}
   * @param logRejectionDetails if {@code true}, every rejected task will be logged with WARN level.
   */
  public ByCallerThreadGroupPolicy(Set<ThreadGroup> waitGroups, ThreadGroup parentGroup, boolean logRejectionDetails) {
    this.waitGroups = unmodifiableSet(waitGroups);
    this.parentGroup = parentGroup;
    this.logRejectionDetails = logRejectionDetails;
  }

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    ThreadGroup targetGroup = ((SchedulerThreadFactory) executor.getThreadFactory()).getGroup();
    ThreadGroup currentThreadGroup = currentThread().getThreadGroup();

    if (isWaitGroupThread(targetGroup) && targetGroup == currentThreadGroup) {
      logRejection(r.toString(), "callerRuns", targetGroup.getName());
      callerRuns.rejectedExecution(r, executor);
    } else if (!isSchedulerThread(currentThreadGroup) || isWaitGroupThread(currentThreadGroup)) {
      logRejection(r.toString(), "wait", targetGroup.getName());
      // MULE-11460 Make CPU-intensive pool a ForkJoinPool - keep the parallelism when waiting.
      wait.rejectedExecution(r, executor);
    } else {
      logRejection(r.toString(), "abort", targetGroup.getName());
      abort.rejectedExecution(r, executor);
    }
  }

  private void logRejection(String taskAsString, String strategy, String targetAsString) {
    if (logRejectionDetails) {
      LOGGER.warn("Task rejected ({}) from '{}' scheduler: {}", rightPad(strategy, 10), targetAsString, taskAsString);
    } else if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Task rejected ({}) from '{}' scheduler: {}", rightPad(strategy, 10), targetAsString, taskAsString);
    }
  }

  private boolean isWaitGroupThread(ThreadGroup threadGroup) {
    if (threadGroup != null) {
      while (threadGroup.getParent() != null) {
        if (waitGroups.contains(threadGroup)) {
          return true;
        } else {
          threadGroup = threadGroup.getParent();
        }
      }
    }
    return false;
  }

  private boolean isSchedulerThread(ThreadGroup threadGroup) {
    if (threadGroup != null) {
      while (threadGroup.getParent() != null) {
        if (threadGroup.equals(parentGroup)) {
          return true;
        } else {
          threadGroup = threadGroup.getParent();
        }
      }
    }
    return false;
  }

}
