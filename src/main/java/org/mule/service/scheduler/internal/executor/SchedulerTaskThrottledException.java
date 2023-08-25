/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal.executor;

import org.mule.service.scheduler.internal.ThrottledScheduler;

import java.util.concurrent.RejectedExecutionException;

/**
 * Exception thrown by a {@link ThrottledScheduler} when all of its threads are busy and it cannot accept a new task for
 * execution.
 *
 * @since 1.0
 */
public class SchedulerTaskThrottledException extends RejectedExecutionException {

  private static final long serialVersionUID = -5085408277835763342L;

  /**
   * Constructs a new exception with the specified message.
   *
   * @param message the detail message
   */
  public SchedulerTaskThrottledException(String message) {
    super(message);
  }

}

