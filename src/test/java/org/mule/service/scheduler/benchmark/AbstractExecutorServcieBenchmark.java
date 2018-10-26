/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.benchmark;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.openjdk.jmh.infra.Blackhole.consumeCPU;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

public abstract class AbstractExecutorServcieBenchmark {

  protected static Callable<Long> CPU_TASK = () -> {
    consumeCPU(2000);
    return currentTimeMillis();
  };

  protected static Callable<Long> BLOCKING_TASK = () -> {
    sleep(200);
    return currentTimeMillis();
  };

  protected static Callable<Long> EMPTY_TASK = () -> {
    return currentTimeMillis();
  };

  public long executeCpuTask(ExecutorService executor) throws Exception {
    return executor.submit(CPU_TASK).get();
  }

  public long executeBlockingTask(ExecutorService executor) throws Exception {
    return executor.submit(BLOCKING_TASK).get();
  }

}
