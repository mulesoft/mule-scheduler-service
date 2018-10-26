/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.benchmark;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Benchmark;
import static org.openjdk.jmh.annotations.Threads.MAX;

import org.mule.service.scheduler.internal.queue.AsyncHandOffQueue;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Microbenchamrk that compares different Executor configurations for the CPU-Lite thread pools.
 */
@Fork(1)
@Warmup(iterations = 10, time = 5, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = SECONDS)
public class ExecutorServiceBlockingQueuesBenchmark {

  @State(Benchmark)
  public static class Subjects {

    private ThreadPoolExecutor sqExecutor;
    private ThreadPoolExecutor jcYieldExecutor;

    @Setup(Level.Trial)
    public void doSetup() {
      int cores = getRuntime().availableProcessors();

      ThreadFactory threadFactory = (ThreadFactory) r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
      };

      RejectedExecutionHandler abortPolicy = (r, executor) -> {
        throw new RejectedExecutionException();
      };

      // Implementation up to 4.1.4
      sqExecutor = new ThreadPoolExecutor(cores * 2, cores * 2, 0L, MILLISECONDS,
                                          new SynchronousQueue<Runnable>(), threadFactory, abortPolicy);

      AsyncHandOffQueue asyncHandoffQueue = new AsyncHandOffQueue(2);
      asyncHandoffQueue.buildHandler((r, executor) -> {
        throw new RejectedExecutionException();
      });

      jcYieldExecutor = new ThreadPoolExecutor(cores * 2, cores * 2, 0L, MILLISECONDS,
                                               asyncHandoffQueue,
                                               threadFactory, asyncHandoffQueue.buildHandler(abortPolicy));
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      sqExecutor.shutdownNow();

      jcYieldExecutor.shutdownNow();
    }

  }

  private static Callable<Long> TASK = () -> {
    sleep(2000);
    return currentTimeMillis();
  };

  @Benchmark
  @Threads(1)
  @BenchmarkMode({AverageTime, Throughput})
  public long sqExecutorSingleThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.sqExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(1)
  @BenchmarkMode({AverageTime, Throughput})
  public long jcYieldExecutorSingleThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.jcYieldExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(MAX)
  @BenchmarkMode({AverageTime, Throughput})
  public long sqExecutorAllThreads(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.sqExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(MAX)
  @BenchmarkMode({AverageTime, Throughput})
  public long jcYieldExecutorAllThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.jcYieldExecutor.submit(TASK).get();
  }

}
