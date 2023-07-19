/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal;

/**
 * Allows for identification of different instances of the same repeatable tasks.
 *
 * 1.0
 */
public interface RepeatableTaskWrapper {

  /**
   * @return the actual repeatable task to be executed
   */
  Runnable getCommand();
}
