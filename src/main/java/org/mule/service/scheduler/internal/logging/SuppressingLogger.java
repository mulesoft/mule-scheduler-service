/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.logging;

import static java.lang.System.currentTimeMillis;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

public class SuppressingLogger {

  private final Logger logger;
  private final long suppressionDuration;
  private final String suppressionSuffix;
  private final AtomicLong lastRejectionLog = new AtomicLong(-1);

  public SuppressingLogger(Logger logger, long suppressionDuration, String suppressionSuffix) {
    this.logger = logger;
    this.suppressionDuration = suppressionDuration;
    this.suppressionSuffix = suppressionSuffix;
  }

  public void log(String message) {
    if (logger.isDebugEnabled()) {
      logger.warn(message);
    } else {
      lastRejectionLog.updateAndGet(t -> {
        long now = currentTimeMillis();
        if (t < now - suppressionDuration) {
          logger.warn("{}. {}", message, suppressionSuffix);

          return now;
        } else {
          return t;
        }
      });
    }
  }
}
