/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.threads;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.lineSeparator;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mule.service.scheduler.ThreadType.CPU_INTENSIVE;
import static org.mule.service.scheduler.ThreadType.CPU_LIGHT;
import static org.mule.service.scheduler.ThreadType.CUSTOM;
import static org.mule.service.scheduler.ThreadType.IO;
import static org.slf4j.LoggerFactory.getLogger;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.lifecycle.LifecycleException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.core.api.scheduler.SchedulerConfig;
import org.mule.runtime.core.api.scheduler.SchedulerPoolsConfig;
import org.mule.runtime.core.api.scheduler.SchedulerService;
import org.mule.service.scheduler.ThreadType;
import org.mule.service.scheduler.internal.DefaultScheduler;
import org.mule.service.scheduler.internal.ThrottledScheduler;
import org.mule.service.scheduler.internal.executor.ByCallerThreadGroupPolicy;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;

/**
 * {@link Scheduler}s provided by this implementation of {@link SchedulerService} use a shared single-threaded
 * {@link ScheduledExecutorService} for scheduling work. When a scheduled tasks is fired, they are executed using the
 * {@link Scheduler}'s own executor.
 * 
 * @since 1.0
 */
public class SchedulerThreadPools {

  private static final Logger logger = getLogger(SchedulerThreadPools.class);

  private static final String CPU_LIGHT_THREADS_NAME = CPU_LIGHT.getName();
  private static final String IO_THREADS_NAME = IO.getName();
  private static final String COMPUTATION_THREADS_NAME = CPU_INTENSIVE.getName();
  private static final String TIMER_THREADS_NAME = "timer";
  private static final String CUSTOM_THREADS_NAME = CUSTOM.getName();

  private String name;
  private SchedulerPoolsConfig threadPoolsConfig;

  private final ThreadGroup schedulerGroup;
  private final ThreadGroup cpuLightGroup;
  private final ThreadGroup ioGroup;
  private final ThreadGroup computationGroup;
  private final ThreadGroup timerGroup;
  private final ThreadGroup customGroup;
  private final ThreadGroup customWaitGroup;

  private final Supplier<RejectedExecutionHandler> byCallerThreadGroupPolicy;

  private ThreadPoolExecutor cpuLightExecutor;
  private ThreadPoolExecutor ioExecutor;
  private ThreadPoolExecutor computationExecutor;
  private Set<ThreadPoolExecutor> customSchedulersExecutors = new HashSet<>();
  private ScheduledThreadPoolExecutor scheduledExecutor;
  private org.quartz.Scheduler quartzScheduler;

  private ReadWriteLock activeSchedulersLock = new ReentrantReadWriteLock();
  private Lock activeSchedulersReadLock = activeSchedulersLock.readLock();
  private Lock activeSchedulersWriteLock = activeSchedulersLock.writeLock();

  private List<Scheduler> activeCpuLightSchedulers = new ArrayList<>();
  private List<Scheduler> activeIoSchedulers = new ArrayList<>();
  private List<Scheduler> activeCpuIntensiveSchedulers = new ArrayList<>();
  private List<Scheduler> activeCustomSchedulers = new ArrayList<>();

  public SchedulerThreadPools(String name, SchedulerPoolsConfig threadPoolsConfig) {
    this.name = name;
    this.threadPoolsConfig = threadPoolsConfig;

    schedulerGroup = new ThreadGroup(name);
    cpuLightGroup = new ThreadGroup(schedulerGroup, threadPoolsConfig.getThreadNamePrefix() + CPU_LIGHT_THREADS_NAME);
    ioGroup = new ThreadGroup(schedulerGroup, threadPoolsConfig.getThreadNamePrefix() + IO_THREADS_NAME);
    computationGroup = new ThreadGroup(schedulerGroup, threadPoolsConfig.getThreadNamePrefix() + COMPUTATION_THREADS_NAME);
    timerGroup = new ThreadGroup(schedulerGroup, threadPoolsConfig.getThreadNamePrefix() + TIMER_THREADS_NAME);
    customGroup = new ThreadGroup(schedulerGroup, threadPoolsConfig.getThreadNamePrefix() + CUSTOM_THREADS_NAME);
    customWaitGroup = new ThreadGroup(customGroup, threadPoolsConfig.getThreadNamePrefix() + CUSTOM_THREADS_NAME);

    final Set<ThreadGroup> waitGroups = new HashSet<>(asList(ioGroup, customWaitGroup));
    byCallerThreadGroupPolicy = () -> new ByCallerThreadGroupPolicy(waitGroups, schedulerGroup);
  }

  public void start() throws MuleException {
    cpuLightExecutor =
        new ThreadPoolExecutor(threadPoolsConfig.getCpuLightPoolSize().getAsInt(),
                               threadPoolsConfig.getCpuLightPoolSize().getAsInt(),
                               0, SECONDS,
                               createQueue(threadPoolsConfig.getCpuLightQueueSize().getAsInt()),
                               new SchedulerThreadFactory(cpuLightGroup), byCallerThreadGroupPolicy.get());
    ioExecutor =
        new ThreadPoolExecutor(threadPoolsConfig.getIoCorePoolSize().getAsInt(), threadPoolsConfig.getIoMaxPoolSize().getAsInt(),
                               threadPoolsConfig.getIoKeepAlive().getAsLong(), MILLISECONDS,
                               // TODO MULE-11505 - Implement cached IO scheduler that grows and uses async hand-off
                               // with queue.
                               new SynchronousQueue<>(),
                               new SchedulerThreadFactory(ioGroup), byCallerThreadGroupPolicy.get());
    computationExecutor =
        new ThreadPoolExecutor(threadPoolsConfig.getCpuIntensivePoolSize().getAsInt(),
                               threadPoolsConfig.getCpuIntensivePoolSize().getAsInt(),
                               0, SECONDS, createQueue(threadPoolsConfig.getCpuIntensiveQueueSize().getAsInt()),
                               new SchedulerThreadFactory(computationGroup), byCallerThreadGroupPolicy.get());

    scheduledExecutor = new ScheduledThreadPoolExecutor(1, new SchedulerThreadFactory(timerGroup, "%s"));
    scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    scheduledExecutor.setRemoveOnCancelPolicy(true);

    StdSchedulerFactory schedulerFactory = new StdSchedulerFactory();
    try {
      schedulerFactory.initialize(defaultQuartzProperties());
      quartzScheduler = schedulerFactory.getScheduler();
      quartzScheduler.start();
    } catch (SchedulerException e) {
      throw new LifecycleException(e, this);
    }

    cpuLightExecutor.prestartAllCoreThreads();
    ioExecutor.prestartAllCoreThreads();
    computationExecutor.prestartAllCoreThreads();
    scheduledExecutor.prestartAllCoreThreads();
  }

  /**
   * Create queue using a {@link SynchronousQueue} if size is 0 or a {@link LinkedBlockingQueue} if size > 0.
   *
   * @param size queue size
   * @return new queue instance
   */
  private BlockingQueue<Runnable> createQueue(int size) {
    return size == 0 ? new SynchronousQueue<>() : new LinkedBlockingQueue<>(size);
  }

  /**
   * @return the properties to provide the quartz scheduler
   */
  private Properties defaultQuartzProperties() {
    Properties factoryProperties = new Properties();

    factoryProperties.setProperty("org.quartz.scheduler.instanceName", threadPoolsConfig.getThreadNamePrefix());
    factoryProperties.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
    factoryProperties.setProperty("org.quartz.threadPool.threadNamePrefix", threadPoolsConfig.getThreadNamePrefix() + "_qz");
    factoryProperties.setProperty("org.quartz.threadPool.threadCount", "1");
    return factoryProperties;
  }

  public void stop() throws MuleException, InterruptedException {
    cpuLightExecutor.shutdown();
    ioExecutor.shutdown();
    computationExecutor.shutdown();
    for (ThreadPoolExecutor customSchedulerExecutor : customSchedulersExecutors) {
      customSchedulerExecutor.shutdown();
    }
    scheduledExecutor.shutdown();
    try {
      quartzScheduler.shutdown(true);
    } catch (SchedulerException e) {
      throw new LifecycleException(e, this);
    }

    final long startMillis = currentTimeMillis();

    // Stop the scheduled first to avoid it dispatching tasks to an already stopped executor
    waitForExecutorTermination(startMillis, scheduledExecutor, threadPoolsConfig.getThreadNamePrefix() + TIMER_THREADS_NAME);
    waitForExecutorTermination(startMillis, cpuLightExecutor, threadPoolsConfig.getThreadNamePrefix() + CPU_LIGHT_THREADS_NAME);
    waitForExecutorTermination(startMillis, ioExecutor, threadPoolsConfig.getThreadNamePrefix() + IO_THREADS_NAME);
    waitForExecutorTermination(startMillis, computationExecutor,
                               threadPoolsConfig.getThreadNamePrefix() + COMPUTATION_THREADS_NAME);

    // When graceful shutdown timeouts, forceful shutdown will remove the custom scheduler from the list.
    // In that case, not creating a new collection here will cause a ConcurrentModificationException.
    for (ThreadPoolExecutor customSchedulerExecutor : new ArrayList<>(customSchedulersExecutors)) {
      waitForExecutorTermination(startMillis, customSchedulerExecutor,
                                 ((SchedulerThreadFactory) customSchedulerExecutor.getThreadFactory()).getGroup().getName());
    }

    cpuLightExecutor = null;
    ioExecutor = null;
    computationExecutor = null;
    scheduledExecutor = null;
    quartzScheduler = null;
  }

  protected void waitForExecutorTermination(final long startMillis, final ExecutorService executor, final String executorLabel)
      throws InterruptedException {
    if (!executor
        .awaitTermination(threadPoolsConfig.getGracefulShutdownTimeout().getAsLong() - (currentTimeMillis() - startMillis),
                          MILLISECONDS)) {
      final List<Runnable> cancelledJobs = executor.shutdownNow();
      logger.warn("'" + executorLabel + "' " + executor.toString() + " did not shutdown gracefully after "
          + threadPoolsConfig.getGracefulShutdownTimeout() + " milliseconds.");

      if (logger.isDebugEnabled()) {
        logger.debug("The jobs " + cancelledJobs + " were cancelled.");
      } else {
        logger.info(cancelledJobs.size() + " jobs were cancelled.");
      }
    }
  }

  public Scheduler createCpuLightScheduler(SchedulerConfig config, int parallelTasksEstimate, Supplier<Long> stopTimeout) {
    validateWaitAllowedNotChanged(config);
    final String schedulerName = resolveCpuLightSchedulerName(config);
    Scheduler scheduler;
    if (shouldThrottle(config, threadPoolsConfig.getCpuLightPoolSize())) {
      scheduler =
          new ThrottledScheduler(schedulerName, cpuLightExecutor, parallelTasksEstimate, scheduledExecutor,
                                 quartzScheduler, CPU_LIGHT, config.getMaxConcurrentTasks(),
                                 stopTimeout, shutdownCallback(activeCpuLightSchedulers));
    } else {
      scheduler = new DefaultScheduler(schedulerName, cpuLightExecutor, parallelTasksEstimate,
                                       scheduledExecutor, quartzScheduler, CPU_LIGHT,
                                       stopTimeout, shutdownCallback(activeCpuLightSchedulers));
    }

    addScheduler(activeCpuLightSchedulers, scheduler);
    return scheduler;
  }

  public Scheduler createIoScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout) {
    validateWaitAllowedNotChanged(config);
    final String schedulerName = resolveIoSchedulerName(config);
    Scheduler scheduler;
    if (shouldThrottle(config, threadPoolsConfig.getIoMaxPoolSize())) {
      scheduler = new ThrottledScheduler(schedulerName, ioExecutor, workers,
                                         scheduledExecutor, quartzScheduler, IO,
                                         config.getMaxConcurrentTasks(), stopTimeout,
                                         shutdownCallback(activeIoSchedulers));
    } else {
      scheduler = new DefaultScheduler(schedulerName, ioExecutor, workers,
                                       scheduledExecutor, quartzScheduler, IO,
                                       stopTimeout, shutdownCallback(activeIoSchedulers));
    }
    addScheduler(activeIoSchedulers, scheduler);
    return scheduler;
  }

  private boolean addScheduler(List<Scheduler> activeSchedulers, Scheduler scheduler) {
    activeSchedulersWriteLock.lock();
    try {
      return activeSchedulers.add(scheduler);
    } finally {
      activeSchedulersWriteLock.unlock();
    }
  }

  private Consumer<Scheduler> shutdownCallback(List<Scheduler> activeSchedulers) {
    return schr -> {
      activeSchedulersWriteLock.lock();
      try {
        activeSchedulers.remove(schr);
      } finally {
        activeSchedulersWriteLock.unlock();
      }
    };
  }

  public Scheduler createCpuIntensiveScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout) {
    validateWaitAllowedNotChanged(config);
    final String schedulerName = resolveComputationSchedulerName(config);
    Scheduler scheduler;
    if (shouldThrottle(config, threadPoolsConfig.getCpuIntensivePoolSize())) {
      scheduler = new ThrottledScheduler(schedulerName, computationExecutor, workers,
                                         scheduledExecutor, quartzScheduler,
                                         CPU_INTENSIVE, config.getMaxConcurrentTasks(), stopTimeout,
                                         shutdownCallback(activeCpuIntensiveSchedulers));
    } else {
      scheduler =
          new DefaultScheduler(schedulerName, computationExecutor, workers, scheduledExecutor,
                               quartzScheduler, CPU_INTENSIVE, stopTimeout,
                               shutdownCallback(activeCpuIntensiveSchedulers));
    }
    addScheduler(activeCpuIntensiveSchedulers, scheduler);
    return scheduler;
  }

  private void validateWaitAllowedNotChanged(SchedulerConfig config) {
    if (config.getWaitAllowed().isPresent()) {
      throw new IllegalArgumentException("Only custom schedulers may define 'waitAllowed' behaviour");
    }
  }

  private boolean shouldThrottle(SchedulerConfig config, OptionalInt backingPoolMaxSize) {
    // Only throttle if max concurrent tasks is less than total backing pool size
    return config.getMaxConcurrentTasks() != null && config.getMaxConcurrentTasks() < backingPoolMaxSize.orElse(MAX_VALUE);
  }

  public Scheduler createCustomScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout) {
    String threadsName = resolveCustomThreadsName(config);
    return doCreateCustomScheduler(config, workers, stopTimeout, resolveCustomSchedulerName(config),
                                   new SynchronousQueue<>(), threadsName);
  }

  public Scheduler createCustomScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout, int queueSize) {
    String threadsName = resolveCustomThreadsName(config);
    return doCreateCustomScheduler(config, workers, stopTimeout, resolveCustomSchedulerName(config),
                                   createQueue(queueSize), threadsName);
  }

  private Scheduler doCreateCustomScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout, String schedulerName,
                                            BlockingQueue<Runnable> workQueue, String threadsName) {
    if (config.getMaxConcurrentTasks() == null) {
      throw new IllegalArgumentException("Custom schedulers must define a thread pool size");
    }

    final ThreadGroup customChildGroup = new ThreadGroup(resolveThreadGroupForCustomScheduler(config), threadsName);
    final ThreadPoolExecutor executor =
        new ThreadPoolExecutor(config.getMaxConcurrentTasks(), config.getMaxConcurrentTasks(), 0L, MILLISECONDS, workQueue,
                               new SchedulerThreadFactory(customChildGroup, "%s.%02d"),
                               byCallerThreadGroupPolicy.get());

    final CustomScheduler customScheduler =
        new CustomScheduler(schedulerName, executor, workers, scheduledExecutor, quartzScheduler, CUSTOM, stopTimeout,
                            shutdownCallback(activeCustomSchedulers));
    customSchedulersExecutors.add(executor);
    addScheduler(activeCustomSchedulers, customScheduler);
    return customScheduler;
  }

  private ThreadGroup resolveThreadGroupForCustomScheduler(SchedulerConfig config) {
    if (config.getWaitAllowed().orElse(false)) {
      return customWaitGroup;
    } else {
      return customGroup;
    }
  }

  private String resolveCpuLightSchedulerName(SchedulerConfig config) {
    if (!config.hasName()) {
      config = config.withName(resolveSchedulerCreationLocation(CPU_LIGHT_THREADS_NAME));
    }
    return config.getSchedulerName();
  }

  private String resolveIoSchedulerName(SchedulerConfig config) {
    if (!config.hasName()) {
      config = config.withName(resolveSchedulerCreationLocation(IO_THREADS_NAME));
    }
    return config.getSchedulerName();
  }

  private String resolveComputationSchedulerName(SchedulerConfig config) {
    if (!config.hasName()) {
      config = config.withName(resolveSchedulerCreationLocation(COMPUTATION_THREADS_NAME));
    }
    return config.getSchedulerName();
  }

  private String resolveCustomSchedulerName(SchedulerConfig config) {
    if (!config.hasName()) {
      config = config.withName(resolveSchedulerCreationLocation(CUSTOM_THREADS_NAME));
    }
    return config.getSchedulerName();
  }

  private String resolveCustomThreadsName(SchedulerConfig config) {
    if (config.hasName()) {
      return config.getSchedulerName();
    } else {
      return threadPoolsConfig.getThreadNamePrefix() + CUSTOM_THREADS_NAME;
    }
  }

  private String resolveSchedulerCreationLocation(String prefix) {
    int i = 0;
    final StackTraceElement[] stackTrace = new Throwable().getStackTrace();

    StackTraceElement ste = stackTrace[i++];
    // We have to go deep enough, right before the proxy calls
    while (skip(ste) && i < stackTrace.length) {
      ste = stackTrace[i++];
    }

    if (skip(ste)) {
      ste = stackTrace[3];
    } else {
      ste = stackTrace[i];
    }

    return prefix + "@" + (ste.getClassName() + "." + ste.getMethodName() + ":" + ste.getLineNumber());
  }

  private boolean skip(StackTraceElement ste) {
    return !ste.getClassName().contains("$Proxy");
  }

  private class CustomScheduler extends DefaultScheduler {

    private final ExecutorService executor;

    private CustomScheduler(String name, ExecutorService executor, int workers, ScheduledExecutorService scheduledExecutor,
                            org.quartz.Scheduler quartzScheduler, ThreadType threadsType, Supplier<Long> shutdownTimeoutMillis,
                            Consumer<Scheduler> shutdownCallback) {
      super(name, executor, workers, scheduledExecutor, quartzScheduler, threadsType, shutdownTimeoutMillis, shutdownCallback);
      this.executor = executor;
    }

    @Override
    public void shutdown() {
      super.shutdown();
      executor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      customSchedulersExecutors.remove(this);

      final List<Runnable> cancelledTasks = super.shutdownNow();
      executor.shutdownNow();
      return cancelledTasks;
    }
  }

  public List<Scheduler> getSchedulers() {
    activeSchedulersReadLock.lock();
    try {
      return ImmutableList.<Scheduler>builder()
          .addAll(activeCpuLightSchedulers)
          .addAll(activeIoSchedulers)
          .addAll(activeCpuIntensiveSchedulers)
          .addAll(activeCustomSchedulers)
          .build();
    } finally {
      activeSchedulersReadLock.unlock();
    }
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
