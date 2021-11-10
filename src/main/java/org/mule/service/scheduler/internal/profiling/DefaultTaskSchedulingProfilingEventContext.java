/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.profiling;

import org.mule.runtime.api.profiling.tracing.ExecutionContext;
import org.mule.runtime.api.profiling.type.context.TaskSchedulingProfilingEventContext;

import java.util.Optional;

public class DefaultTaskSchedulingProfilingEventContext implements TaskSchedulingProfilingEventContext {

  private final long triggerTimestamp;
  private final String taskId;
  private final String threadName;
  private final ExecutionContext executionContext;

  public DefaultTaskSchedulingProfilingEventContext(long triggerTimestamp, String taskId, String threadName,
                                                    ExecutionContext executionContext) {
    this.triggerTimestamp = triggerTimestamp;
    this.taskId = taskId;
    this.threadName = threadName;
    this.executionContext = executionContext;
  }

  @Override
  public long getTriggerTimestamp() {
    return triggerTimestamp;
  }

  @Override
  public Optional<ExecutionContext> getTaskTracingContext() {
    return Optional.ofNullable(executionContext);
  }

  @Override
  public String getTaskId() {
    return taskId;
  }

  @Override
  public String getThreadName() {
    return threadName;
  }
}
