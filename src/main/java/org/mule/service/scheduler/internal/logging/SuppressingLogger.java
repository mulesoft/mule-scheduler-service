/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
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
