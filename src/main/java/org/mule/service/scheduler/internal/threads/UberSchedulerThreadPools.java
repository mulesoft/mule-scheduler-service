/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.threads;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mule.service.scheduler.ThreadType.IO;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerConfig;
import org.mule.runtime.api.scheduler.SchedulerPoolsConfig;
import org.mule.service.scheduler.internal.DefaultScheduler;
import org.mule.service.scheduler.internal.ThrottledScheduler;
import org.mule.service.scheduler.internal.executor.ByCallerThreadGroupPolicy;
import org.mule.service.scheduler.internal.executor.ByCallerThrottlingPolicy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;

/**
 * {@link SchedulerThreadPools} implementation that uses one single pool for cpuLight, IO and cpuIntensive pools, while
 * maintaining a separate one for the custom ones.
 *
 * @since 1.3.0
 */
class UberSchedulerThreadPools extends SchedulerThreadPools {

  private static final String UBER_THREADS_NAME = "uber";

  private final List<Scheduler> activeSchedulers = new ArrayList<>();

  private ThreadGroup uberGroup;
  private ThreadPoolExecutor uberExecutor;

  public UberSchedulerThreadPools(String name, SchedulerPoolsConfig threadPoolsConfig,
                                  boolean preStartThreads,
                                  Consumer<ThreadPoolExecutor> preStartCallback,
                                  Logger traceLogger) {
    super(name, threadPoolsConfig, preStartThreads, preStartCallback, traceLogger);
  }

  @Override
  protected void doStart(boolean preStartThreads) throws MuleException {
    // TODO (elrodro83) MULE-14203 Make IO thread pool have an optimal core size
    uberExecutor = new ThreadPoolExecutor(threadPoolsConfig.getUberCorePoolSize().getAsInt(),
                                          threadPoolsConfig.getUberMaxPoolSize().getAsInt(),
                                          threadPoolsConfig.getUberKeepAlive().getAsLong(), MILLISECONDS,
                                          // At first, it may seem that a SynchronousQueue is not the best option here since it may
                                          // block the dispatching thread, which may be a CPU-light.
                                          // However, the alternatives have some limitations that make them impractical:
                                          //
                                          // * Using a LinkedBlockingQueue causes the pool not to grow until the queue is full. This
                                          // causes unwanted delays in the processing if the core size of the pool is small, or
                                          // keeping too many idle threads if the core size is large.
                                          //
                                          // * Using a custom SynchronizedQueue + RejectedExectuionHandler
                                          // (https://gist.github.com/elrodro83/96e1ee470237a57fb06376a7e4b04f2b) that addresses the
                                          // limitations of the other 2 approaches, an improvement is seen in the dispatching of the
                                          // tasks, but at the cost of a slower task taking, which slows down the processing so much
                                          // that it greatly outweights the gain in the dispatcher.
                                          createQueue(threadPoolsConfig.getUberQueueSize().getAsInt()),
                                          new SchedulerThreadFactory(uberGroup),
                                          byCallerThreadGroupPolicy.apply(uberGroup.getName()));

    if (preStartThreads) {
      prestartCoreThreads(uberExecutor, threadPoolsConfig.getUberCorePoolSize().getAsInt());
    }
  }

  @Override
  public Scheduler createCpuLightScheduler(SchedulerConfig config, int parallelTasksEstimate, Supplier<Long> stopTimeout) {
    return createIoScheduler(config, parallelTasksEstimate, stopTimeout);
  }

  @Override
  public Scheduler createIoScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout) {
    validateCustomSchedulerOnlyConfigNotChanged(config);
    final String schedulerName = resolveSchedulerName(config, UBER_THREADS_NAME);
    Scheduler scheduler;
    if (shouldThrottle(config, threadPoolsConfig.getUberMaxPoolSize())) {
      scheduler =
          new ThrottledScheduler(schedulerName, uberExecutor, workers, scheduledExecutor, quartzScheduler, IO,
                                 new ByCallerThrottlingPolicy(config.getMaxConcurrentTasks(),
                                                              new HashSet<>(asList(uberGroup, customWaitGroup)),
                                                              parentGroup,
                                                              traceLogger),
                                 stopTimeout, shutdownCallback(activeSchedulers));
    } else {
      scheduler = new DefaultScheduler(schedulerName, uberExecutor, workers,
                                       scheduledExecutor, quartzScheduler, IO,
                                       stopTimeout, shutdownCallback(activeSchedulers));
    }
    addScheduler(activeSchedulers, scheduler);
    return scheduler;
  }

  @Override
  public Scheduler createCpuIntensiveScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout) {
    return createIoScheduler(config, workers, stopTimeout);
  }


  @Override
  protected void createCustomThreadGroups() {
    uberGroup = new ThreadGroup(parentGroup, threadPoolsConfig.getThreadNamePrefix() + UBER_THREADS_NAME);
  }

  @Override
  protected ByCallerThreadGroupPolicy createThreadGroupPolicy(String schedulerName) {
    final Set<ThreadGroup> waitGroups = new HashSet<>(asList(uberGroup, customWaitGroup, customCallerRunsAnsWaitGroup));

    return new ByCallerThreadGroupPolicy(waitGroups,
                                         new HashSet<>(asList(customCallerRunsGroup, customCallerRunsAnsWaitGroup)),
                                         uberGroup, parentGroup, schedulerName, traceLogger);
  }

  @Override
  protected ThreadPoolExecutor getCustomSchedulerDestroyerExecutor() {
    return uberExecutor;
  }

  @Override
  protected void shutdownPools() throws MuleException, InterruptedException {
    uberExecutor.shutdown();
  }

  @Override
  protected void waitForExecutorTermination(long shutdownStartMillis) throws InterruptedException {
    waitForExecutorTermination(shutdownStartMillis, uberExecutor, threadPoolsConfig.getThreadNamePrefix() + UBER_THREADS_NAME);
  }

  @Override
  protected void onStopCompleted() {
    uberExecutor = null;
  }

  @Override
  public boolean isCurrentThreadForCpuWork() {
    return currentThread().getThreadGroup() == uberGroup;
  }

  @Override
  protected List<Scheduler> getOwnSchedulers() {
    return new ArrayList<>(activeSchedulers);
  }

  public String buildReportString() {
    final StringBuilder threadPoolsReportBuilder = new StringBuilder();

    int uberSchedulers;
    int schedulersCustom;

    activeSchedulersReadLock.lock();
    try {
      uberSchedulers = activeSchedulers.size();
      schedulersCustom = activeCustomSchedulers.size();
    } finally {
      activeSchedulersReadLock.unlock();
    }

    final int activeCount = uberExecutor.getActiveCount();
    final long taskCount = uberExecutor.getTaskCount();
    final long rejectedCount = ((ByCallerThreadGroupPolicy) uberExecutor.getRejectedExecutionHandler()).getRejectedCount();

    int customActiveCount = 0;
    int customUsedCount = 0;
    int customQueued = 0;
    long customTaskCount = 0;
    long customRejected = 0;
    for (ThreadPoolExecutor customExecutor : customSchedulersExecutors) {
      final int currentCustomActive = customExecutor.getActiveCount();
      customActiveCount += currentCustomActive;
      customUsedCount += customExecutor.getPoolSize() - currentCustomActive;
      customQueued += customExecutor.getQueue().size();
      customTaskCount += customExecutor.getTaskCount();
      customRejected += ((ByCallerThreadGroupPolicy) customExecutor.getRejectedExecutionHandler()).getRejectedCount();
    }

    threadPoolsReportBuilder.append(lineSeparator() + name + lineSeparator());
    threadPoolsReportBuilder
        .append("--------------------------------------------------------------------------------------" + lineSeparator());
    threadPoolsReportBuilder
        .append("Pool          | Schedulers | Idle threads | Used threads | Queued tasks | Rejection % " + lineSeparator());
    threadPoolsReportBuilder
        .append("--------------------------------------------------------------------------------------" + lineSeparator());
    threadPoolsReportBuilder
        .append(format("Uber            | %10d | %12d | %12d | %12d | ~ %9.2f",
                       uberSchedulers,
                       uberExecutor.getPoolSize() - activeCount,
                       activeCount,
                       uberExecutor.getQueue().size(),
                       rejectedCount > 0 ? 100.0 * (rejectedCount / (taskCount + rejectedCount)) : 0)
            + lineSeparator());
    threadPoolsReportBuilder
        .append(format("Custom        | %10d | %12d | %12d | %12d | ~ %9.2f",
                       schedulersCustom,
                       customUsedCount,
                       customActiveCount,
                       customQueued,
                       customRejected > 0 ? 100.0 * (customRejected / (customTaskCount + customRejected)) : 0)
            + lineSeparator());
    threadPoolsReportBuilder
        .append("--------------------------------------------------------------------------------------" + lineSeparator()
            + lineSeparator());

    return threadPoolsReportBuilder.toString();
  }
}
