/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal.util;

import java.util.function.Consumer;

/**
 * Utility to force an indirection in a method call.
 */
public class Delegator implements Consumer<Runnable> {

  @Override
  public void accept(Runnable r) {
    r.run();
  }

}
