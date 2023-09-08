/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
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
