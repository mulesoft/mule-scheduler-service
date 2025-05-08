/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.reporting;

import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.service.scheduler.ThreadType;

import java.util.concurrent.Executor;

/**
 * 
 */
public interface ReportableScheduler extends Scheduler {

  /**
   * 
   * @return
   */
  public Executor getActualExecutor();

  public ThreadType getThreadType();

}
