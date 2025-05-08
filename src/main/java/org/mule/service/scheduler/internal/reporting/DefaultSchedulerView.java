/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.reporting;

import static java.lang.Integer.toHexString;

import org.mule.runtime.api.scheduler.SchedulerView;

/**
 * Basic implementation of {@link SchedulerView}.
 *
 * @since 1.0
 */
public class DefaultSchedulerView implements SchedulerView {

  private ReportableScheduler scheduler;

  /**
   * Creates a reporting view for a {@link ReportableScheduler}.
   *
   * @param scheduler the scheduler to provide a view for.
   */
  public DefaultSchedulerView(ReportableScheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public String getName() {
    return scheduler.getName();
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return scheduler.isTerminated();
  }

  @Override
  public String getActualExecutorId() {
    return toHexString(scheduler.getActualExecutor().hashCode());
  }

  @Override
  public String getActualExecutorToString() {
    return scheduler.getActualExecutor().toString();
  }

  @Override
  public String getThreadType() {
    return scheduler.getThreadType().name();
  }

  @Override
  public String toString() {
    return scheduler.toString();
  }
}
