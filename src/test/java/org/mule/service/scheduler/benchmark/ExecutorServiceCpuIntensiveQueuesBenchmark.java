/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.benchmark;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Benchmark;
import static org.openjdk.jmh.annotations.Threads.MAX;
import static org.openjdk.jmh.infra.Blackhole.consumeCPU;

import org.mule.service.scheduler.internal.executor.WaitPolicy;
import org.mule.service.scheduler.internal.queue.CustomBlockingYieldMpmcQueue;

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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Microbenchamrk that compares different Executor configurations for the CPU-Intensive thread pools.
 */
@Fork(1)
@Warmup(iterations = 10, time = 5, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = SECONDS)
public class ExecutorServiceCpuIntensiveQueuesBenchmark {

  @State(Benchmark)
  public static class Subjects {

    private ThreadPoolExecutor lbqExecutor;
    private ThreadPoolExecutor abqExecutor;
    private ThreadPoolExecutor jcYieldExecutor;

    @Setup(Level.Trial)
    public void doSetup() {
      int cores = getRuntime().availableProcessors();

      ThreadFactory threadFactory = (ThreadFactory) r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
      };

      RejectedExecutionHandler waitPolicy = new WaitPolicy((r, executor) -> {
        throw new RejectedExecutionException();
      }, "scheduler");

      // Implementation up to 4.1.4
      lbqExecutor = new ThreadPoolExecutor(cores * 2, cores * 2, 0L, MILLISECONDS,
                                           new LinkedBlockingQueue<Runnable>(cores * 2), threadFactory, waitPolicy);

      abqExecutor = new ThreadPoolExecutor(cores * 2, cores * 2, 0L, MILLISECONDS,
                                           new ArrayBlockingQueue<Runnable>(cores * 2), threadFactory, waitPolicy);
      jcYieldExecutor = new ThreadPoolExecutor(cores * 2, cores * 2, 0L, MILLISECONDS,
                                               new CustomBlockingYieldMpmcQueue<Runnable>(cores * 2),
                                               threadFactory, waitPolicy);
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      lbqExecutor.shutdownNow();
      abqExecutor.shutdownNow();

      jcYieldExecutor.shutdownNow();
    }

  }

  private static Callable<Long> TASK = () -> {
    consumeCPU(2000);
    return currentTimeMillis();
  };

  @Benchmark
  @Threads(1)
  @BenchmarkMode({AverageTime, Throughput})
  public long lbqExecutorSingleThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.lbqExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(1)
  @BenchmarkMode({AverageTime, Throughput})
  public long abqExecutorSingleThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.abqExecutor.submit(TASK).get();
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
  public long lbqExecutorAllThreads(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.lbqExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(MAX)
  @BenchmarkMode({AverageTime, Throughput})
  public long abqExecutorAllThreads(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.abqExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(MAX)
  @BenchmarkMode({AverageTime, Throughput})
  public long jcYieldExecutorAllThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.jcYieldExecutor.submit(TASK).get();
  }

}
