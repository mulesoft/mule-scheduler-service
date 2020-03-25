/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.threads;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.getProperties;
import static java.lang.System.lineSeparator;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.lang.Thread.yield;
import static java.util.Arrays.asList;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static java.util.concurrent.ForkJoinPool.getCommonPoolParallelism;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.SystemUtils.IS_JAVA_1_8;
import static org.mule.runtime.api.i18n.I18nMessageFactory.createStaticMessage;
import static org.mule.runtime.api.util.MuleSystemProperties.SYSTEM_PROPERTY_PREFIX;
import static org.mule.service.scheduler.ThreadType.CPU_INTENSIVE;
import static org.mule.service.scheduler.ThreadType.CPU_LIGHT;
import static org.mule.service.scheduler.ThreadType.CUSTOM;
import static org.mule.service.scheduler.ThreadType.IO;
import static org.slf4j.LoggerFactory.getLogger;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.exception.MuleRuntimeException;
import org.mule.runtime.api.lifecycle.LifecycleException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerConfig;
import org.mule.runtime.api.scheduler.SchedulerPoolsConfig;
import org.mule.runtime.api.scheduler.SchedulerService;
import org.mule.service.scheduler.ThreadType;
import org.mule.service.scheduler.internal.DefaultScheduler;
import org.mule.service.scheduler.internal.ThrottledScheduler;
import org.mule.service.scheduler.internal.executor.ByCallerThreadGroupPolicy;
import org.mule.service.scheduler.internal.executor.ByCallerThrottlingPolicy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;

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

  /**
   * If set, the location where an Scheduler was created will be put in the name of the Scheduler always, regardless of whether
   * the schedulerConfig has a name already or not.
   */
  private static final boolean ALWAYS_SHOW_SCHEDULER_CREATION_LOCATION =
      getProperties().containsKey(SYSTEM_PROPERTY_PREFIX + "scheduler.alwaysShowSchedulerCreationLocation");

  private final String name;
  private final SchedulerPoolsConfig threadPoolsConfig;

  private final ThreadGroup schedulerGroup;
  private final ThreadGroup cpuLightGroup;
  private final ThreadGroup ioGroup;
  private final ThreadGroup computationGroup;
  private final ThreadGroup timerGroup;
  private final ThreadGroup customGroup;
  private final ThreadGroup customWaitGroup;
  private final ThreadGroup customCallerRunsGroup;
  private final ThreadGroup customCallerRunsAnsWaitGroup;

  private final Function<String, RejectedExecutionHandler> byCallerThreadGroupPolicy;
  private final Predicate<ThreadGroup> cpuWorkChecker;

  private ThreadPoolExecutor cpuLightExecutor;
  private ThreadPoolExecutor ioExecutor;
  private ThreadPoolExecutor computationExecutor;
  private final Set<ThreadPoolExecutor> customSchedulersExecutors = new HashSet<>();
  private ScheduledThreadPoolExecutor scheduledExecutor;
  private org.quartz.Scheduler quartzScheduler;

  private final ReadWriteLock activeSchedulersLock = new ReentrantReadWriteLock();
  private final Lock activeSchedulersReadLock = activeSchedulersLock.readLock();
  private final Lock activeSchedulersWriteLock = activeSchedulersLock.writeLock();

  private final List<Scheduler> activeCpuLightSchedulers = new ArrayList<>();
  private final List<Scheduler> activeIoSchedulers = new ArrayList<>();
  private final List<Scheduler> activeCpuIntensiveSchedulers = new ArrayList<>();
  private final List<Scheduler> activeCustomSchedulers = new ArrayList<>();

  public SchedulerThreadPools(String name, SchedulerPoolsConfig threadPoolsConfig) {
    this.name = name;
    this.threadPoolsConfig = threadPoolsConfig;

    schedulerGroup = new ThreadGroup(name) {

      @Override
      public void uncaughtException(Thread t, Throwable e) {
        // This case happens if some rogue code stops our threads.
        logger.error("Thread '" + t.getName() + "' stopped.", e);
      }
    };
    cpuLightGroup = new ThreadGroup(schedulerGroup, threadPoolsConfig.getThreadNamePrefix() + CPU_LIGHT_THREADS_NAME);
    ioGroup = new ThreadGroup(schedulerGroup, threadPoolsConfig.getThreadNamePrefix() + IO_THREADS_NAME);
    computationGroup = new ThreadGroup(schedulerGroup, threadPoolsConfig.getThreadNamePrefix() + COMPUTATION_THREADS_NAME);
    timerGroup = new ThreadGroup(schedulerGroup, threadPoolsConfig.getThreadNamePrefix() + TIMER_THREADS_NAME);
    customGroup = new ThreadGroup(schedulerGroup, threadPoolsConfig.getThreadNamePrefix() + CUSTOM_THREADS_NAME);
    customWaitGroup = new ThreadGroup(customGroup, threadPoolsConfig.getThreadNamePrefix() + CUSTOM_THREADS_NAME);
    customCallerRunsGroup = new ThreadGroup(customGroup, threadPoolsConfig.getThreadNamePrefix() + CUSTOM_THREADS_NAME);
    customCallerRunsAnsWaitGroup = new ThreadGroup(customGroup, threadPoolsConfig.getThreadNamePrefix() + CUSTOM_THREADS_NAME);

    final Set<ThreadGroup> waitGroups = new HashSet<>(asList(ioGroup, customWaitGroup, customCallerRunsAnsWaitGroup));
    final Set<ThreadGroup> cpuWorkGroups = new HashSet<>(asList(cpuLightGroup, computationGroup));

    byCallerThreadGroupPolicy = schedulerName -> new ByCallerThreadGroupPolicy(waitGroups,
                                                                               new HashSet<>(asList(cpuLightGroup,
                                                                                                    computationGroup,
                                                                                                    customCallerRunsGroup,
                                                                                                    customCallerRunsAnsWaitGroup)),
                                                                               cpuLightGroup, schedulerGroup, schedulerName);

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

  public void start() throws MuleException {
    // Workaround to avoid the leak caused by https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8172726
    if (IS_JAVA_1_8) {
      prestartCoreThreads(commonPool(), getCommonPoolParallelism());
    }

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

    prestartCoreThreads(cpuLightExecutor, threadPoolsConfig.getCpuLightPoolSize().getAsInt());
    prestartCoreThreads(ioExecutor, threadPoolsConfig.getIoCorePoolSize().getAsInt());
    prestartCoreThreads(computationExecutor, threadPoolsConfig.getCpuIntensivePoolSize().getAsInt());

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
    factoryProperties.setProperty("org.quartz.jobStore.misfireThreshold", "" + SECONDS.toMillis(5));
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
    validateCustomSchedulerOnlyConfigNotChanged(config);
    final String schedulerName = resolveCpuLightSchedulerName(config);
    Scheduler scheduler;
    if (shouldThrottle(config, threadPoolsConfig.getCpuLightPoolSize())) {
      scheduler =
          new ThrottledScheduler(schedulerName, cpuLightExecutor, parallelTasksEstimate, scheduledExecutor, quartzScheduler,
                                 CPU_LIGHT,
                                 new ByCallerThrottlingPolicy(config.getMaxConcurrentTasks(),
                                                              new HashSet<>(asList(ioGroup, customWaitGroup)), schedulerGroup),
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
    validateCustomSchedulerOnlyConfigNotChanged(config);
    final String schedulerName = resolveIoSchedulerName(config);
    Scheduler scheduler;
    if (shouldThrottle(config, threadPoolsConfig.getIoMaxPoolSize())) {
      scheduler =
          new ThrottledScheduler(schedulerName, ioExecutor, workers, scheduledExecutor, quartzScheduler, IO,
                                 new ByCallerThrottlingPolicy(config.getMaxConcurrentTasks(),
                                                              new HashSet<>(asList(ioGroup, customWaitGroup)), schedulerGroup),
                                 stopTimeout, shutdownCallback(activeIoSchedulers));
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
    validateCustomSchedulerOnlyConfigNotChanged(config);
    final String schedulerName = resolveComputationSchedulerName(config);
    Scheduler scheduler;
    if (shouldThrottle(config, threadPoolsConfig.getCpuIntensivePoolSize())) {
      scheduler =
          new ThrottledScheduler(schedulerName, computationExecutor, workers, scheduledExecutor, quartzScheduler, CPU_INTENSIVE,
                                 new ByCallerThrottlingPolicy(config.getMaxConcurrentTasks(),
                                                              new HashSet<>(asList(ioGroup, customWaitGroup)), schedulerGroup),
                                 stopTimeout, shutdownCallback(activeCpuIntensiveSchedulers));
    } else {
      scheduler = new DefaultScheduler(schedulerName, computationExecutor, workers, scheduledExecutor,
                                       quartzScheduler, CPU_INTENSIVE, stopTimeout,
                                       shutdownCallback(activeCpuIntensiveSchedulers));
    }
    addScheduler(activeCpuIntensiveSchedulers, scheduler);
    return scheduler;
  }

  private void validateCustomSchedulerOnlyConfigNotChanged(SchedulerConfig config) {
    if (config.getWaitAllowed().isPresent()) {
      throw new IllegalArgumentException("Only custom schedulers may define 'waitAllowed' behaviour");
    }
    if (config.getDirectRunCpuLightWhenTargetBusy().isPresent()) {
      throw new IllegalArgumentException("Only custom schedulers may define 'directRunCpuLightWhenTargetBusy' behaviour");
    }
  }

  private boolean shouldThrottle(SchedulerConfig config, OptionalInt backingPoolMaxSize) {
    // Only throttle if max concurrent tasks is less than total backing pool size
    return config.getMaxConcurrentTasks() != null && config.getMaxConcurrentTasks() < backingPoolMaxSize.orElse(MAX_VALUE);
  }

  public Scheduler createCustomScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout) {
    String threadsName = resolveCustomThreadsName(config);
    return doCreateCustomScheduler(config, workers, stopTimeout, resolveCustomSchedulerName(config),
                                   // SynchronousQueue may reject tasks early right after creation/finish of the previous task
                                   createQueue(config.getMaxConcurrentTasks()),
                                   threadsName);
  }

  public Scheduler createCustomScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout, int queueSize) {
    String threadsName = resolveCustomThreadsName(config);
    return doCreateCustomScheduler(config, workers, stopTimeout, resolveCustomSchedulerName(config),
                                   createQueue(queueSize), threadsName);
  }

  private Scheduler doCreateCustomScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout, String schedulerName,
                                            BlockingQueue<Runnable> workQueue, String threadsName) {
    if (config.getMaxConcurrentTasks() == null) {
      throw new IllegalArgumentException("Custom schedulers must define a thread pool size bi calling `config.withMaxConcurrentTasks()`");
    }

    final ThreadGroup customChildGroup = new ThreadGroup(resolveThreadGroupForCustomScheduler(config), threadsName);
    final ThreadPoolExecutor executor =
        new ThreadPoolExecutor(config.getMaxConcurrentTasks(), config.getMaxConcurrentTasks(), 0L, MILLISECONDS, workQueue,
                               new SchedulerThreadFactory(customChildGroup, "%s.%02d"),
                               byCallerThreadGroupPolicy.apply(customChildGroup.getName()));

    prestartCoreThreads(executor, config.getMaxConcurrentTasks());

    Set<ThreadPoolExecutor> executors = customSchedulersExecutors;

    final CustomScheduler customScheduler =
        new CustomScheduler(schedulerName, executor, customChildGroup, workers, scheduledExecutor, quartzScheduler, ioExecutor,
                            CUSTOM, stopTimeout,
                            shutdownCallback(activeCustomSchedulers).andThen(s -> executors.remove(executor)));
    executors.add(executor);
    addScheduler(activeCustomSchedulers, customScheduler);
    return customScheduler;
  }

  /**
   * Workaround to avoid a race condition when a {@link SynchronousQueue} is combined with a call to
   * {@link ThreadPoolExecutor#prestartAllCoreThreads()}.
   * <p>
   * When using a SynchronousQueue on an Executor, even if calling prestartAllCoreThreads(), a race condition may occur where the
   * worker threads are started but before it starts to take elements from the queue, the user of the Executor dispatches some
   * task to it.
   * <p>
   * In that case, the threads are prestarted by dispatching work to the executor as a workaround, which avoids the issue.
   *
   * @param executor the executor to prestart the threads for
   * @param corePoolSize the number of threads to start in e=the {@code executor}
   */
  private void prestartCoreThreads(final AbstractExecutorService executor, int corePoolSize) {
    // This latch is to ensure that the required executions are active at the same time
    CountDownLatch prestartWaitLatch = new CountDownLatch(1);
    // This latch is to validate that the required executions have actually run
    CountDownLatch prestartLatch = new CountDownLatch(corePoolSize);
    // The futures are kept to ensure that the prestart tasks are done before continuing
    List<Future<?>> prestartFutures = new ArrayList<>(corePoolSize);

    final String executorAsString = executor.toString();

    for (int i = 0; i < corePoolSize; ++i) {
      try {
        prestartFutures.add(executor.submit(() -> {
          try {
            prestartWaitLatch.await();
          } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new MuleRuntimeException(e);
          }
          prestartCallback(prestartLatch);
        }));
      } catch (RejectedExecutionException ree) {
        executor.shutdownNow();
        throw new MuleRuntimeException(createStaticMessage("Unable to prestart all core threads for executor:"
            + executorAsString));
      }
    }

    prestartWaitLatch.countDown();
    try {
      if (!prestartLatch.await(30, SECONDS)) {
        executor.shutdownNow();
        throw new MuleRuntimeException(createStaticMessage("Unable to prestart all core threads for executor:"
            + executorAsString));
      }

      try {
        for (Future<?> future : prestartFutures) {
          future.get(30, SECONDS);
        }
      } catch (ExecutionException e) {
        throw new MuleRuntimeException(createStaticMessage("Unable to prestart all core threads for executor:"
            + executorAsString), e.getCause());
      } catch (TimeoutException e) {
        throw new MuleRuntimeException(createStaticMessage("Unable to prestart all core threads for executor:"
            + executorAsString), e);
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
      throw new MuleRuntimeException(e);
    }
  }

  protected void prestartCallback(CountDownLatch prestartLatch) {
    prestartLatch.countDown();
  }

  private ThreadGroup resolveThreadGroupForCustomScheduler(SchedulerConfig config) {
    if (config.getDirectRunCpuLightWhenTargetBusy().orElse(false) && config.getWaitAllowed().orElse(false)) {
      return customCallerRunsAnsWaitGroup;
    } else if (config.getDirectRunCpuLightWhenTargetBusy().orElse(false)) {
      return customCallerRunsGroup;
    } else if (config.getWaitAllowed().orElse(false)) {
      return customWaitGroup;
    } else {
      return customGroup;
    }
  }

  private String resolveCpuLightSchedulerName(SchedulerConfig config) {
    return resolveSchedulerName(config, CPU_LIGHT_THREADS_NAME);
  }

  private String resolveIoSchedulerName(SchedulerConfig config) {
    return resolveSchedulerName(config, IO_THREADS_NAME);
  }

  private String resolveComputationSchedulerName(SchedulerConfig config) {
    return resolveSchedulerName(config, COMPUTATION_THREADS_NAME);
  }

  private String resolveCustomSchedulerName(SchedulerConfig config) {
    return resolveSchedulerName(config, CUSTOM_THREADS_NAME);
  }

  private String resolveSchedulerName(SchedulerConfig config, String prefix) {
    if (!config.hasName()) {
      config = config.withName(resolveSchedulerCreationLocation(prefix));
      return config.getSchedulerName();
    } else if (ALWAYS_SHOW_SCHEDULER_CREATION_LOCATION) {
      return config.getSchedulerName() + " " + resolveSchedulerCreationLocation(null);
    } else {
      return config.getSchedulerName();
    }
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
      ste = stackTrace[4];
    } else {
      ste = stackTrace[i];
    }

    return (prefix != null ? prefix : "") + "@" + ste.getClassName() + "." + ste.getMethodName() + ":" + ste.getLineNumber();
  }

  private boolean skip(StackTraceElement ste) {
    return !ste.getClassName().contains("$Proxy");
  }

  public boolean isCurrentThreadForCpuWork() {
    return cpuWorkChecker.test(currentThread().getThreadGroup());
  }

  private static class CustomScheduler extends DefaultScheduler {

    private static final float THREADS_IN_GROUP_SIZE_MARGIN = 1.5f;

    private final ExecutorService executor;
    private final ThreadGroup threadGroup;

    private final ThreadPoolExecutor groupDestroyerExecutor;

    private CustomScheduler(String name, ExecutorService executor, ThreadGroup threadGroup, int workers,
                            ScheduledExecutorService scheduledExecutor, org.quartz.Scheduler quartzScheduler,
                            ThreadPoolExecutor groupDestroyerExecutor,
                            ThreadType threadsType, Supplier<Long> shutdownTimeoutMillis, Consumer<Scheduler> shutdownCallback) {
      super(name, executor, workers, scheduledExecutor, quartzScheduler, threadsType, shutdownTimeoutMillis,
            shutdownCallback);
      this.executor = executor;
      this.threadGroup = threadGroup;
      this.groupDestroyerExecutor = groupDestroyerExecutor;
    }

    @Override
    public void shutdown() {
      logger.debug("Shutting down " + this.toString());
      doShutdown();
      executor.shutdown();
      shutdownCallback.accept(this);
    }

    @Override
    public List<Runnable> shutdownNow() {
      logger.debug("Shutting down NOW " + this.toString());
      try {
        List<Runnable> cancelledTasks = doShutdownNow();
        executor.shutdownNow();
        return cancelledTasks;
      } finally {
        shutdownWrapUp();
      }
    }

    @Override
    protected void stopFinally() {
      executor.shutdownNow();
      shutdownWrapUp();
    }

    private void shutdownWrapUp() {
      shutdownCallback.accept(this);

      if (threadGroup.equals(currentThread().getThreadGroup())) {
        // Avoid thread suicide
        groupDestroyerExecutor.execute(() -> destroyThreadGroup());
      } else {
        destroyThreadGroup();
      }
    }

    private void destroyThreadGroup() {
      IllegalThreadStateException destroyException = doDestroyThreadGroup();

      if (destroyException != null) {
        threadGroup.interrupt();
        destroyException = doDestroyThreadGroup();
      }

      tryTerminate();

      if (destroyException != null) {
        // Create the array larger in case new threads are created after the enumeration
        Thread[] threads = new Thread[(int) (threadGroup.activeCount() * THREADS_IN_GROUP_SIZE_MARGIN)];
        threadGroup.enumerate(threads, true);
        StringBuilder threadNamesBuilder = new StringBuilder();
        for (Thread thread : threads) {
          // Account for the extra slots added to the array
          if (thread == null) {
            continue;
          }

          threadNamesBuilder.append("\t* " + thread.getName() + lineSeparator());

          if (logger.isDebugEnabled()) {
            final StackTraceElement[] stackTrace = thread.getStackTrace();
            for (int i = 1; i < stackTrace.length; i++) {
              threadNamesBuilder.append("\t\tat ").append(stackTrace[i]).append(lineSeparator());
            }
          }
        }

        logger.error("Unable to destroy ThreadGroup '{}' of Scheduler '{}' ({}). Remaining threads in the group are:"
            + lineSeparator() + "{}", threadGroup.getName(), this.getName(), destroyException.toString(), threadNamesBuilder);
      }
    }

    private IllegalThreadStateException doDestroyThreadGroup() {
      IllegalThreadStateException destroyException = null;

      final long durationMillis = shutdownTimeoutMillis.get();
      final long stopNanos = nanoTime() + MILLISECONDS.toNanos(shutdownTimeoutMillis.get()) + SECONDS.toNanos(1);
      while (nanoTime() <= stopNanos && !threadGroup.isDestroyed()) {
        try {
          threadGroup.destroy();
          destroyException = null;
          break;
        } catch (IllegalThreadStateException e) {
          // The wrapup of the threads is done asynchronously by java, so we perform this repeatedly until it runs after the
          // wrapup (ref: Thread#exit()).
          // If after the specified timeout still cannot be destroyed, the the exception is thrown.
          destroyException = e;
          try {
            yield();
            sleep(min(50, durationMillis));
          } catch (InterruptedException e1) {
            currentThread().interrupt();
            break;
          }
        }
      }
      return destroyException;
    }

    @Override
    public String getThreadNameSuffix() {
      return null;
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
