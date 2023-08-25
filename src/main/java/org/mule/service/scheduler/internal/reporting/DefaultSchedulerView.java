/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal.reporting;

import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerView;

/**
 * Basic implementation of {@link SchedulerView}.
 *
 * @since 1.0
 */
public class DefaultSchedulerView implements SchedulerView {

  private Scheduler scheduler;

  /**
   * Creates a reporting view for a {@link Scheduler}.
   *
   * @param scheduler the scheduler to provide a view for.
   */
  public DefaultSchedulerView(Scheduler scheduler) {
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
  public String toString() {
    return scheduler.toString();
  }
}
