/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.threads;

import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;

import static java.lang.Thread.MAX_PRIORITY;
import static java.util.Optional.of;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import io.qameta.allure.Feature;
import org.junit.Test;

@Feature(SCHEDULER_SERVICE)
public class SchedulerThreadFactoryTestCase {

  private static final Runnable DO_NOTHING = () -> {
  };

  @Test
  public void threadPriority() {
    ThreadGroup threadGroup = new ThreadGroup("TestGroup");
    SchedulerThreadFactory threadFactory = new SchedulerThreadFactory(threadGroup, "%s.%02d", of(MAX_PRIORITY));
    Thread thread = threadFactory.newThread(DO_NOTHING);

    assertThat(thread.getPriority(), is(MAX_PRIORITY));
  }
}
