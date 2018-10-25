/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Benchmark;
import static org.openjdk.jmh.annotations.Threads.MAX;
import static org.openjdk.jmh.infra.Blackhole.consumeCPU;

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

import com.bluedevel.concurrent.CustomBlockingMpmcQueue;
import com.bluedevel.concurrent.CustomBlockingYieldMpmcQueue;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

@Fork(1)
@Warmup(iterations = 10, time = 5, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = SECONDS)
public class ExecutorServiceVersusDisruptor {

  @State(Benchmark)
  public static class Subjects {

    private ThreadPoolExecutor sqExecutor;
    private ThreadPoolExecutor abqExecutor;
    private ThreadPoolExecutor disruptorWlExecutor;
    private ThreadPoolExecutor disruptorExecutor;
    private ThreadPoolExecutor jcSleepExecutor;
    private ThreadPoolExecutor jcYieldExecutor;

    private Disruptor<CompletableFutureEvent> swDisruptor;
    private Disruptor<CompletableFutureEvent> ywDisruptor;
    // private ExecutorService disruptorExecutor;
    private RingBuffer<CompletableFutureEvent> swRingBuffer;
    private RingBuffer<CompletableFutureEvent> ywRingBuffer;

    private static class CompletableFutureEvent {

      CompletableFuture<Long> future;

    }

    @Setup(Level.Trial)
    public void doSetup() {
      int cores = getRuntime().availableProcessors();

      ThreadFactory threadFactory = (ThreadFactory) r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
      };

      sqExecutor = new ThreadPoolExecutor(cores * 2, cores * 2, 0L, MILLISECONDS,
                                          new SynchronousQueue<Runnable>(), threadFactory);
      abqExecutor = new ThreadPoolExecutor(cores * 2, cores * 2, 0L, MILLISECONDS,
                                           new ArrayBlockingQueue<Runnable>(cores), threadFactory);
      disruptorWlExecutor = new ThreadPoolExecutor(cores * 2, cores * 2, 0L, MILLISECONDS,
                                                   new DisruptorBlockingQueue<Runnable>(cores, true),
                                                   threadFactory);
      disruptorExecutor = new ThreadPoolExecutor(cores * 2, cores * 2, 0L, MILLISECONDS,
                                                 new DisruptorBlockingQueue<Runnable>(cores, false),
                                                 threadFactory);

      jcSleepExecutor = new ThreadPoolExecutor(cores * 2, cores * 2, 0L, MILLISECONDS,
                                               new CustomBlockingMpmcQueue<Runnable>(cores),
                                               threadFactory);
      jcYieldExecutor = new ThreadPoolExecutor(cores * 2, cores * 2, 0L, MILLISECONDS,
                                               new CustomBlockingYieldMpmcQueue<Runnable>(cores),
                                               threadFactory);

      // Executor that will be used to construct new threads for consumers
      // Executor executor = Executors.newCachedThreadPool();

      // Specify the size of the ring buffer, must be power of 2.
      int bufferSize = cores * 2;

      // Construct the Disruptor
      // Disruptor<LongEvent> disruptor = new Disruptor<>(LongEvent::new, bufferSize, executor);
      // disruptorExecutor = newCachedThreadPool();

      // disruptor = new Disruptor<>(MyEvent::new, bufferSize, (ThreadFactory) r -> new Thread(r), ProducerType.MULTI,
      // new YieldingWaitStrategy());

      // Connect the handlers
      EventHandler<? super CompletableFutureEvent> eventHandler = (event, sequence, endOfBatch) -> {
        event.future.complete(TASK.call());
      };
      EventHandler<? super CompletableFutureEvent>[] eventHandlers = new EventHandler[cores * 2];
      for (int i = 0; i < cores * 2; ++i) {
        eventHandlers[i] = eventHandler;
      }

      swDisruptor =
          new Disruptor<>(CompletableFutureEvent::new, bufferSize, (ThreadFactory) r -> new Thread(r), ProducerType.MULTI,
                          new SleepingWaitStrategy());

      swDisruptor.handleEventsWith(eventHandlers);

      // Start the Disruptor, starts all threads running
      swDisruptor.start();

      // Get the ring buffer from the Disruptor to be used for publishing.
      swRingBuffer = swDisruptor.getRingBuffer();

      ywDisruptor =
          new Disruptor<>(CompletableFutureEvent::new, bufferSize, (ThreadFactory) r -> new Thread(r), ProducerType.MULTI,
                          new YieldingWaitStrategy());

      ywDisruptor.handleEventsWith(eventHandlers);

      // Start the Disruptor, starts all threads running
      ywDisruptor.start();

      // Get the ring buffer from the Disruptor to be used for publishing.
      ywRingBuffer = ywDisruptor.getRingBuffer();

      // for (long l = 0; true; l++) {
      // ringBuffer.publishEvent((event, sequence, timestamp) -> event.setTimestampIn(timestamp), currentTimeMillis());
      // Thread.sleep(1000);
      // }
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
      sqExecutor.shutdownNow();
      abqExecutor.shutdownNow();
      disruptorExecutor.shutdownNow();
      disruptorWlExecutor.shutdownNow();

      jcSleepExecutor.shutdownNow();
      jcYieldExecutor.shutdownNow();

      swDisruptor.shutdown();
      ywDisruptor.shutdown();
    }

  }

  private static Callable<Long> TASK = () -> {
    consumeCPU(2000);
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
  public long abqExecutorSingleThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.abqExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(1)
  @BenchmarkMode({AverageTime, Throughput})
  public long disruptorExecutorSingleThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.disruptorExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(1)
  @BenchmarkMode({AverageTime, Throughput})
  public long disruptorWlExecutorSingleThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.disruptorWlExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(1)
  @BenchmarkMode({AverageTime, Throughput})
  public long jcSleepExecutorSingleThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.jcSleepExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(1)
  @BenchmarkMode({AverageTime, Throughput})
  public long jcYieldExecutorSingleThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.jcYieldExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(1)
  @BenchmarkMode({AverageTime, Throughput})
  public long swDisruptorSingleThread(Subjects subjects) throws InterruptedException, ExecutionException {
    CompletableFuture<Long> future = new CompletableFuture<>();
    subjects.swRingBuffer.publishEvent((event, sequence, f) -> {
      event.future = f;
    }, future);
    return future.get();
  }

  @Benchmark
  @Threads(1)
  @BenchmarkMode({AverageTime, Throughput})
  public long ywDisruptorSingleThread(Subjects subjects) throws InterruptedException, ExecutionException {
    CompletableFuture<Long> future = new CompletableFuture<>();
    subjects.ywRingBuffer.publishEvent((event, sequence, f) -> {
      event.future = f;
    }, future);
    return future.get();
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
  public long abqExecutorAllThreads(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.abqExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(MAX)
  @BenchmarkMode({AverageTime, Throughput})
  public long disruptorExecutorAllThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.disruptorExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(MAX)
  @BenchmarkMode({AverageTime, Throughput})
  public long disruptorWlExecutorAllThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.disruptorWlExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(MAX)
  @BenchmarkMode({AverageTime, Throughput})
  public long jcSleepExecutorAllThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.jcSleepExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(MAX)
  @BenchmarkMode({AverageTime, Throughput})
  public long jcYieldExecutorAllThread(Subjects subjects) throws InterruptedException, ExecutionException {
    return subjects.jcYieldExecutor.submit(TASK).get();
  }

  @Benchmark
  @Threads(MAX)
  @BenchmarkMode({AverageTime, Throughput})
  public long swDisruptorAllThreads(Subjects subjects) throws InterruptedException, ExecutionException {
    CompletableFuture<Long> future = new CompletableFuture<>();
    subjects.swRingBuffer.publishEvent((event, sequence, f) -> {
      event.future = f;
    }, future);
    return future.get();
  }

  @Benchmark
  @Threads(MAX)
  @BenchmarkMode({AverageTime, Throughput})
  public long ywDisruptorAllThreads(Subjects subjects) throws InterruptedException, ExecutionException {
    CompletableFuture<Long> future = new CompletableFuture<>();
    subjects.ywRingBuffer.publishEvent((event, sequence, f) -> {
      event.future = f;
    }, future);
    return future.get();
  }
}
