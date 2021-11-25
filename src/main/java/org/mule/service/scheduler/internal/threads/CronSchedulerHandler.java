/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.threads;

import org.mule.runtime.api.util.concurrent.Latch;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Creates and holds a unique cron scheduler instance.
 */
public class CronSchedulerHandler {

  private static final int WAIT_TIMEOUT = 5000;

  private final ThreadGroup threadGroup;
  private final String threadNamePrefix;
  private final Latch latch = new Latch();
  private volatile Throwable exception = null;
  private volatile org.quartz.Scheduler quartzScheduler = null;
  private final Runnable schedulerCreator = () -> {
    StdSchedulerFactory schedulerFactory = new StdSchedulerFactory();
    try {
      schedulerFactory.initialize(defaultQuartzProperties());
      quartzScheduler = schedulerFactory.getScheduler();
    } catch (SchedulerException e) {
      exception = e;
    } finally {
      latch.release();
    }
  };

  public CronSchedulerHandler(ThreadGroup threadGroup, String threadNamePrefix) {
    this.threadNamePrefix = threadNamePrefix;
    this.threadGroup = threadGroup;
  }

  public org.quartz.Scheduler getScheduler() throws SchedulerException, InterruptedException {
    if (quartzScheduler == null) {
      synchronized (CronSchedulerHandler.class) {
        if (quartzScheduler == null) {
          /*
           * We need to set the thread group of the quartz scheduler thread pool threads to a custom one. The only way to achieve
           * it is to make them inherit the thread group of the thread creating the scheduler, thus we need to create the
           * scheduler in a thread that belongs to that custom thread group.
           */
          new Thread(threadGroup, schedulerCreator, "CronSchedulerHandler").start();
          if (!latch.await(WAIT_TIMEOUT, MILLISECONDS)) {
            throw new RuntimeException("Quartz scheduler creation timed out");
          }
          if (exception != null) {
            throw (SchedulerException) exception;
          }
        }
      }
    }

    return quartzScheduler;
  }

  /**
   * @return the properties to provide the quartz scheduler
   */
  private Properties defaultQuartzProperties() {
    Properties factoryProperties = new Properties();

    factoryProperties.setProperty("org.quartz.scheduler.instanceName", threadNamePrefix);
    factoryProperties.setProperty("org.quartz.threadPool.threadsInheritGroupOfInitializingThread", "true");
    factoryProperties.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
    factoryProperties.setProperty("org.quartz.threadPool.threadNamePrefix", threadNamePrefix + "_qz");
    factoryProperties.setProperty("org.quartz.threadPool.threadCount", "1");
    factoryProperties.setProperty("org.quartz.jobStore.misfireThreshold", "" + SECONDS.toMillis(5));
    return factoryProperties;
  }
}
