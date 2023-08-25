/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal.executor;

import static java.util.Collections.unmodifiableSet;

import org.mule.runtime.api.scheduler.SchedulerService;
import org.mule.service.scheduler.ThreadType;

import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;

/**
 * Provides base functionality to take actions based on the {@link ThreadType} of the threads used when dispatching tasks.
 *
 * @since 1.0
 */
public abstract class AbstractByCallerPolicy {

  private final Set<ThreadGroup> waitGroups;
  private final Set<ThreadGroup> runCpuLightWhenTargetBusyGroups;
  private final ThreadGroup parentGroup;

  /**
   * Builds a new {@link AbstractByCallerPolicy} with the given {@code waitGroups}.
   *
   * @param waitGroups                      the group of threads for which a {@link WaitPolicy} will be applied. For the rest, an
   *                                        {@link AbortPolicy} will be applied.
   * @param runCpuLightWhenTargetBusyGroups the group of threads for which a {@link CallerRunsPolicy} will be applied when
   *                                        dispatching to cpu-light.
   * @param parentGroup                     the {@link SchedulerService} parent {@link ThreadGroup}
   */
  protected AbstractByCallerPolicy(Set<ThreadGroup> waitGroups, Set<ThreadGroup> runCpuLightWhenTargetBusyGroups,
                                   ThreadGroup parentGroup) {
    this.waitGroups = unmodifiableSet(waitGroups);
    this.runCpuLightWhenTargetBusyGroups = unmodifiableSet(runCpuLightWhenTargetBusyGroups);
    this.parentGroup = parentGroup;
  }

  protected boolean isWaitGroupThread(ThreadGroup threadGroup) {
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

  protected boolean isRunCpuLightWhenTargetBusyThread(ThreadGroup threadGroup) {
    if (threadGroup != null) {
      while (threadGroup.getParent() != null) {
        if (runCpuLightWhenTargetBusyGroups.contains(threadGroup)) {
          return true;
        } else {
          threadGroup = threadGroup.getParent();
        }
      }
    }
    return false;
  }

  protected boolean isSchedulerThread(ThreadGroup threadGroup) {
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
