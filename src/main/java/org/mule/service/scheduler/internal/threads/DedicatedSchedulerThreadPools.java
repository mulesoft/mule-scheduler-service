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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mule.service.scheduler.ThreadType.CPU_INTENSIVE;
import static org.mule.service.scheduler.ThreadType.CPU_LIGHT;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.slf4j.Logger;

class DedicatedSchedulerThreadPools extends SchedulerThreadPools {

  private static final String CPU_LIGHT_THREADS_NAME = CPU_LIGHT.getName();
  private static final String IO_THREADS_NAME = IO.getName();
  private static final String COMPUTATION_THREADS_NAME = CPU_INTENSIVE.getName();

  private ThreadGroup cpuLightGroup;
  private ThreadGroup ioGroup;
  private ThreadGroup computationGroup;

  private ThreadPoolExecutor cpuLightExecutor;
  private ThreadPoolExecutor ioExecutor;
  private ThreadPoolExecutor computationExecutor;

  private Predicate<ThreadGroup> cpuWorkChecker;

  private final List<Scheduler> activeCpuLightSchedulers = new ArrayList<>();
  private final List<Scheduler> activeIoSchedulers = new ArrayList<>();
  private final List<Scheduler> activeCpuIntensiveSchedulers = new ArrayList<>();

  public DedicatedSchedulerThreadPools(String name, SchedulerPoolsConfig threadPoolsConfig,
                                       boolean preStartThreads,
                                       Consumer<ThreadPoolExecutor> preStartCallback,
                                       Logger traceLogger) {
    super(name, threadPoolsConfig, preStartThreads, preStartCallback, traceLogger);
  }

  @Override
  protected void doStart(boolean preStartThreads) throws MuleException {
    cpuLightExecutor =
        new ThreadPoolExecutor(threadPoolsConfig.getCpuLightPoolSize().getAsInt(),
                               threadPoolsConfig.getCpuLightPoolSize().getAsInt(),
                               0, SECONDS,
                               createQueue(threadPoolsConfig.getCpuLightQueueSize().getAsInt()),
                               new SchedulerThreadFactory(cpuLightGroup),
                               byCallerThreadGroupPolicy.apply(cpuLightGroup.getName()));

    // TODO (elrodro83) MULE-14203 Make IO thread pool have an optimal core size
    ioExecutor = new ThreadPoolExecutor(threadPoolsConfig.getIoCorePoolSize().getAsInt(),
                                        threadPoolsConfig.getIoMaxPoolSize().getAsInt(),
                                        threadPoolsConfig.getIoKeepAlive().getAsLong(), MILLISECONDS,
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
                                        createQueue(threadPoolsConfig.getIoQueueSize().getAsInt()),
                                        new SchedulerThreadFactory(ioGroup),
                                        byCallerThreadGroupPolicy.apply(ioGroup.getName()));
    computationExecutor =
        new ThreadPoolExecutor(threadPoolsConfig.getCpuIntensivePoolSize().getAsInt(),
                               threadPoolsConfig.getCpuIntensivePoolSize().getAsInt(),
                               0, SECONDS, createQueue(threadPoolsConfig.getCpuIntensiveQueueSize().getAsInt()),
                               new SchedulerThreadFactory(computationGroup),
                               byCallerThreadGroupPolicy.apply(computationGroup.getName()));

    if (preStartThreads) {
      prestartCoreThreads(cpuLightExecutor, threadPoolsConfig.getCpuLightPoolSize().getAsInt());
      prestartCoreThreads(ioExecutor, threadPoolsConfig.getIoCorePoolSize().getAsInt());
      prestartCoreThreads(computationExecutor, threadPoolsConfig.getCpuIntensivePoolSize().getAsInt());
    }
  }

  @Override
  public Scheduler createCpuLightScheduler(SchedulerConfig config, int parallelTasksEstimate, Supplier<Long> stopTimeout) {
    validateCustomSchedulerOnlyConfigNotChanged(config);
    final String schedulerName = resolveCpuLightSchedulerName(config);
    Scheduler scheduler;
    if (shouldThrottle(config, threadPoolsConfig.getCpuLightPoolSize())) {
      scheduler =
          new ThrottledScheduler(schedulerName, cpuLightExecutor, parallelTasksEstimate, scheduledExecutor, quartzScheduler,
                                 CPU_LIGHT,
                                 new ByCallerThrottlingPolicy(config.getMaxConcurrentTasks(),
                                                              new HashSet<>(asList(ioGroup, customWaitGroup)),
                                                              parentGroup,
                                                              traceLogger),
                                 stopTimeout, shutdownCallback(activeCpuLightSchedulers));
    } else {
      scheduler = new DefaultScheduler(schedulerName, cpuLightExecutor, parallelTasksEstimate,
                                       scheduledExecutor, quartzScheduler, CPU_LIGHT,
                                       stopTimeout, shutdownCallback(activeCpuLightSchedulers));
    }

    addScheduler(activeCpuLightSchedulers, scheduler);
    return scheduler;
  }

  @Override
  public Scheduler createIoScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout) {
    validateCustomSchedulerOnlyConfigNotChanged(config);
    final String schedulerName = resolveIoSchedulerName(config);
    Scheduler scheduler;
    if (shouldThrottle(config, threadPoolsConfig.getIoMaxPoolSize())) {
      scheduler =
          new ThrottledScheduler(schedulerName, ioExecutor, workers, scheduledExecutor, quartzScheduler, IO,
                                 new ByCallerThrottlingPolicy(config.getMaxConcurrentTasks(),
                                                              new HashSet<>(asList(ioGroup, customWaitGroup)),
                                                              parentGroup,
                                                              traceLogger),
                                 stopTimeout, shutdownCallback(activeIoSchedulers));
    } else {
      scheduler = new DefaultScheduler(schedulerName, ioExecutor, workers,
                                       scheduledExecutor, quartzScheduler, IO,
                                       stopTimeout, shutdownCallback(activeIoSchedulers));
    }
    addScheduler(activeIoSchedulers, scheduler);
    return scheduler;
  }

  @Override
  public Scheduler createCpuIntensiveScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout) {
    validateCustomSchedulerOnlyConfigNotChanged(config);
    final String schedulerName = resolveComputationSchedulerName(config);
    Scheduler scheduler;
    if (shouldThrottle(config, threadPoolsConfig.getCpuIntensivePoolSize())) {
      scheduler =
          new ThrottledScheduler(schedulerName, computationExecutor, workers, scheduledExecutor, quartzScheduler, CPU_INTENSIVE,
                                 new ByCallerThrottlingPolicy(config.getMaxConcurrentTasks(),
                                                              new HashSet<>(asList(ioGroup, customWaitGroup)),
                                                              parentGroup,
                                                              traceLogger),
                                 stopTimeout, shutdownCallback(activeCpuIntensiveSchedulers));
    } else {
      scheduler = new DefaultScheduler(schedulerName, computationExecutor, workers, scheduledExecutor,
                                       quartzScheduler, CPU_INTENSIVE, stopTimeout,
                                       shutdownCallback(activeCpuIntensiveSchedulers));
    }
    addScheduler(activeCpuIntensiveSchedulers, scheduler);
    return scheduler;
  }


  @Override
  protected void
  createCustomThreadGroups() {
    cpuLightGroup = new ThreadGroup(parentGroup, threadPoolsConfig.getThreadNamePrefix() + CPU_LIGHT_THREADS_NAME);
    ioGroup = new ThreadGroup(parentGroup, threadPoolsConfig.getThreadNamePrefix() + IO_THREADS_NAME);
    computationGroup = new ThreadGroup(parentGroup, threadPoolsConfig.getThreadNamePrefix() + COMPUTATION_THREADS_NAME);

    final Set<ThreadGroup> cpuWorkGroups = new HashSet<>(asList(cpuLightGroup, computationGroup));
    cpuWorkChecker = threadGroup -> {
      if (threadGroup != null) {
        while (threadGroup.getParent() != null) {
          if (cpuWorkGroups.contains(threadGroup)) {
            return true;
          } else {
            threadGroup = threadGroup.getParent();
          }
        }
      }
      return false;
    };

  }

  @Override
  protected ByCallerThreadGroupPolicy createThreadGroupPolicy(String schedulerName) {
    final Set<ThreadGroup> waitGroups = new HashSet<>(asList(ioGroup, customWaitGroup, customCallerRunsAnsWaitGroup));

    return new ByCallerThreadGroupPolicy(waitGroups,
                                         new HashSet<>(asList(cpuLightGroup,
                                                              computationGroup,
                                                              customCallerRunsGroup,
                                                              customCallerRunsAnsWaitGroup)),
                                         cpuLightGroup, parentGroup, schedulerName,
                                         traceLogger);

  }

  @Override
  protected ThreadPoolExecutor getCustomerSchedulerExecutor() {
    return ioExecutor;
  }

  @Override
  protected void shutdownPools() throws MuleException, InterruptedException {
    cpuLightExecutor.shutdown();
    ioExecutor.shutdown();
    computationExecutor.shutdown();
  }

  @Override
  protected void waitForExecutorTermination(long shutdownStartMillis) throws InterruptedException {
    waitForExecutorTermination(shutdownStartMillis, cpuLightExecutor, threadPoolsConfig.getThreadNamePrefix() + CPU_LIGHT_THREADS_NAME);
    waitForExecutorTermination(shutdownStartMillis, ioExecutor, threadPoolsConfig.getThreadNamePrefix() + IO_THREADS_NAME);
    waitForExecutorTermination(shutdownStartMillis, computationExecutor,
                               threadPoolsConfig.getThreadNamePrefix() + COMPUTATION_THREADS_NAME);
  }

  @Override
  protected void onStopCompleted() {
    cpuLightExecutor = null;
    ioExecutor = null;
    computationExecutor = null;
  }

  @Override
  public boolean isCurrentThreadForCpuWork() {
    return cpuWorkChecker.test(currentThread().getThreadGroup());
  }

  @Override
  protected String resolveCpuLightSchedulerName(SchedulerConfig config) {
    return resolveSchedulerName(config, CPU_LIGHT_THREADS_NAME);
  }

  @Override
  protected String resolveIoSchedulerName(SchedulerConfig config) {
    return resolveSchedulerName(config, IO_THREADS_NAME);
  }

  @Override
  protected String resolveComputationSchedulerName(SchedulerConfig config) {
    return resolveSchedulerName(config, COMPUTATION_THREADS_NAME);
  }

  @Override
  protected List<Scheduler> getOwnSchedulers() {
    List<Scheduler> schedulers = new LinkedList<>();
    schedulers.addAll(activeCpuLightSchedulers);
    schedulers.addAll(activeIoSchedulers);
    schedulers.addAll(activeCpuIntensiveSchedulers);

    return schedulers;
  }

  public String buildReportString() {
    final StringBuilder threadPoolsReportBuilder = new StringBuilder();

    int schedulersCpuLight;
    int schedulersIo;
    int schedulersCpuIntensive;
    int schedulersCustom;

    activeSchedulersReadLock.lock();
    try {
      schedulersCpuLight = activeCpuLightSchedulers.size();
      schedulersIo = activeIoSchedulers.size();
      schedulersCpuIntensive = activeCpuIntensiveSchedulers.size();
      schedulersCustom = activeCustomSchedulers.size();
    } finally {
      activeSchedulersReadLock.unlock();
    }

    final int cpuLightActiveCount = cpuLightExecutor.getActiveCount();
    final long cpuLightTaskCount = cpuLightExecutor.getTaskCount();
    final long cpuLightRejected = ((ByCallerThreadGroupPolicy) cpuLightExecutor.getRejectedExecutionHandler()).getRejectedCount();
    final int ioActiveCount = ioExecutor.getActiveCount();
    final long ioTaskCount = ioExecutor.getTaskCount();
    final long ioRejected = ((ByCallerThreadGroupPolicy) ioExecutor.getRejectedExecutionHandler()).getRejectedCount();
    final int cpuIntensiveActiveCount = computationExecutor.getActiveCount();
    final long cpuIntensiveTaskCount = computationExecutor.getTaskCount();
    final long cpuIntensiveRejected =
        ((ByCallerThreadGroupPolicy) computationExecutor.getRejectedExecutionHandler()).getRejectedCount();

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
        .append(format("CPU Light     | %10d | %12d | %12d | %12d | ~ %9.2f",
                       schedulersCpuLight,
                       cpuLightExecutor.getPoolSize() - cpuLightActiveCount,
                       cpuLightActiveCount,
                       cpuLightExecutor.getQueue().size(),
                       cpuLightRejected > 0 ? 100.0 * (cpuLightRejected / (cpuLightTaskCount + cpuLightRejected)) : 0)
                    + lineSeparator());
    threadPoolsReportBuilder
        .append(format("IO            | %10d | %12d | %12d | %12d | ~ %9.2f",
                       schedulersIo,
                       ioExecutor.getPoolSize() - ioActiveCount,
                       ioActiveCount,
                       ioExecutor.getQueue().size(),
                       ioRejected > 0 ? 100.0 * (ioRejected / (ioTaskCount + ioRejected)) : 0)
                    + lineSeparator());
    threadPoolsReportBuilder
        .append(format("CPU Intensive | %10d | %12d | %12d | %12d | ~ %9.2f",
                       schedulersCpuIntensive,
                       computationExecutor.getPoolSize() - cpuIntensiveActiveCount,
                       cpuIntensiveActiveCount,
                       computationExecutor.getQueue().size(),
                       cpuIntensiveRejected > 0 ? 100.0 * (cpuIntensiveRejected / (cpuIntensiveTaskCount + cpuIntensiveRejected))
                           : 0)
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
