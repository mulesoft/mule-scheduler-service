/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;

@PersistJobDataAfterExecution
public class QuartzCronJob implements Job {

  public static final String JOB_TASK_KEY = QuartzCronJob.class.getName() + ".task";

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    ((Runnable) context.getJobDetail().getJobDataMap().get(JOB_TASK_KEY)).run();
  }

}
