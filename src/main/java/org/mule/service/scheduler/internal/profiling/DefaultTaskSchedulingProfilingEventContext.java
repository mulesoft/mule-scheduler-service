/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.profiling;

import org.mule.runtime.api.profiling.tracing.TaskTracingContext;
import org.mule.runtime.api.profiling.type.context.TaskSchedulingProfilingEventContext;

public class DefaultTaskSchedulingProfilingEventContext implements TaskSchedulingProfilingEventContext {

  private final long triggerTimestamp;
  private final TaskTracingContext taskTracingContext;

  public DefaultTaskSchedulingProfilingEventContext(long triggerTimestamp, TaskTracingContext taskTracingContext) {
    this.triggerTimestamp = triggerTimestamp;
    this.taskTracingContext = taskTracingContext;
  }

  @Override
  public long getTriggerTimestamp() {
    return triggerTimestamp;
  }

  @Override
  public TaskTracingContext getTaskTracingContext() {
    return taskTracingContext;
  }
}
