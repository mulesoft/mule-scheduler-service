/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;

import io.qameta.allure.Feature;

@Feature(SCHEDULER_SERVICE)
public class DefaultSchedulerServiceTestCase extends SchedulerServiceContractTestCase {

  @Override
  protected SchedulerServiceAdapter createSchedulerService() {
    return new DefaultSchedulerService();
  }
}
