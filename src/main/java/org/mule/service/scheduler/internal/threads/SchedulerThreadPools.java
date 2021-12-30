/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.threads;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.getProperties;
import static java.lang.System.lineSeparator;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.lang.Thread.yield;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static java.util.concurrent.ForkJoinPool.getCommonPoolParallelism;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.SystemUtils.IS_JAVA_1_8;
import static org.mule.runtime.api.i18n.I18nMessageFactory.createStaticMessage;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.DEDICATED;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.UBER;
import static org.mule.runtime.api.util.MuleSystemProperties.SYSTEM_PROPERTY_PREFIX;
import static org.mule.service.scheduler.ThreadType.CUSTOM;
import static org.slf4j.LoggerFactory.getLogger;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.exception.MuleRuntimeException;
import org.mule.runtime.api.lifecycle.LifecycleException;
import org.mule.runtime.api.profiling.ProfilingService;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerConfig;
import org.mule.runtime.api.scheduler.SchedulerPoolsConfig;
import org.mule.runtime.api.scheduler.SchedulerService;
import org.mule.service.scheduler.ThreadType;
import org.mule.service.scheduler.internal.DefaultScheduler;
import org.mule.service.scheduler.internal.executor.ByCallerThreadGroupPolicy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalInt;
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
import java.util.function.Supplier;

import org.quartz.SchedulerException;
import org.slf4j.Logger;

/**
 * {@link Scheduler}s provided by this implementation of {@link SchedulerService} use a shared single-threaded
 * {@link ScheduledExecutorService} for scheduling work. When a scheduled tasks is fired, they are executed using the
 * {@link Scheduler}'s own executor.
 *
 * @since 1.0
 */
public abstract class SchedulerThreadPools {

  private static final Logger LOGGER = getLogger(SchedulerThreadPools.class);


  public static class Builder {

    private final String name;
    private final SchedulerPoolsConfig threadPoolsConfig;
    private boolean preStartThreads = true;
    private Logger traceLogger = LOGGER;
    private Consumer<AbstractExecutorService> preStartCallback = executor -> {
    };

    private Builder(String name, SchedulerPoolsConfig threadPoolsConfig) {
      this.name = name;
      this.threadPoolsConfig = threadPoolsConfig;
    }

    public Builder preStartThreads(boolean prestart) {
      this.preStartThreads = prestart;
      return this;
    }

    public Builder setTraceLogger(Logger traceLogger) {
      this.traceLogger = traceLogger;
      return this;
    }

    public Builder setPreStartCallback(Consumer<AbstractExecutorService> preStartCallback) {
      this.preStartCallback = preStartCallback;
      return this;
    }

    public SchedulerThreadPools build() {
      if (threadPoolsConfig.getSchedulerPoolStrategy() == DEDICATED) {
        return new DedicatedSchedulerThreadPools(name,
                                                 threadPoolsConfig,
                                                 preStartThreads,
                                                 preStartCallback,
                                                 traceLogger);
      } else if (threadPoolsConfig.getSchedulerPoolStrategy() == UBER) {
        return new UberSchedulerThreadPools(name,
                                            threadPoolsConfig,
                                            preStartThreads,
                                            preStartCallback,
                                            traceLogger);
      } else {
        throw new IllegalArgumentException("Unsupported pool strategy type " + threadPoolsConfig.getSchedulerPoolStrategy());
      }
    }

  }

  public static Builder builder(String name, SchedulerPoolsConfig config) {
    return new Builder(name, config);
  }

  private static final String TIMER_THREADS_NAME = "timer";
  private static final String CUSTOM_THREADS_NAME = CUSTOM.getName();

  /**
   * If set, the location where an Scheduler was created will be put in the name of the Scheduler always, regardless of whether
   * the schedulerConfig has a name already or not.
   */
  private static final boolean ALWAYS_SHOW_SCHEDULER_CREATION_LOCATION =
      getProperties().containsKey(SYSTEM_PROPERTY_PREFIX + "scheduler.alwaysShowSchedulerCreationLocation");

  private final boolean preStartThreads;
  private final Consumer<AbstractExecutorService> preStartCallback;

  private final ReadWriteLock activeSchedulersLock = new ReentrantReadWriteLock();

  protected final String name;
  protected final ThreadGroup parentGroup;
  protected final SchedulerPoolsConfig threadPoolsConfig;
  protected final ThreadGroup timerGroup;
  protected final ThreadGroup customGroup;
  protected final ThreadGroup customWaitGroup;
  protected final ThreadGroup customCallerRunsGroup;
  protected final ThreadGroup customCallerRunsAnsWaitGroup;
  protected final Set<ThreadPoolExecutor> customSchedulersExecutors = new HashSet<>();
  protected final Function<String, RejectedExecutionHandler> byCallerThreadGroupPolicy;
  protected final Lock activeSchedulersReadLock = activeSchedulersLock.readLock();
  protected final List<Scheduler> activeCustomSchedulers = new ArrayList<>();
  protected final Lock activeSchedulersWriteLock = activeSchedulersLock.writeLock();
  protected final Logger traceLogger;

  protected ScheduledThreadPoolExecutor scheduledExecutor;
  private CronSchedulerHandler cronSchedulerHandler;
  protected org.quartz.Scheduler quartzScheduler;

  protected SchedulerThreadPools(String name,
                                 SchedulerPoolsConfig threadPoolsConfig,
                                 boolean preStartThreads,
                                 Consumer<AbstractExecutorService> preStartCallback,
                                 Logger traceLogger) {
    this.name = name;
    this.threadPoolsConfig = threadPoolsConfig;
    this.preStartThreads = preStartThreads;
    this.preStartCallback = preStartCallback;
    this.traceLogger = traceLogger;

    parentGroup = new ThreadGroup(name) {

      @Override
      public void uncaughtException(Thread t, Throwable e) {
        // This case happens if some rogue code stops our threads.
        LOGGER.error("Thread '" + t.getName() + "' stopped.", e);
      }
    };

    createCustomThreadGroups();

    timerGroup = new ThreadGroup(parentGroup, threadPoolsConfig.getThreadNamePrefix() + TIMER_THREADS_NAME);
    customGroup = new ThreadGroup(parentGroup, threadPoolsConfig.getThreadNamePrefix() + CUSTOM_THREADS_NAME);
    customWaitGroup = new ThreadGroup(customGroup, threadPoolsConfig.getThreadNamePrefix() + CUSTOM_THREADS_NAME);
    customCallerRunsGroup = new ThreadGroup(customGroup, threadPoolsConfig.getThreadNamePrefix() + CUSTOM_THREADS_NAME);
    customCallerRunsAnsWaitGroup = new ThreadGroup(customGroup, threadPoolsConfig.getThreadNamePrefix() + CUSTOM_THREADS_NAME);

    byCallerThreadGroupPolicy = this::createThreadGroupPolicy;
  }

  protected abstract ByCallerThreadGroupPolicy createThreadGroupPolicy(String schedulerName);

  protected abstract void createCustomThreadGroups();

  public final void start() throws MuleException {
    // Workaround to avoid the leak caused by https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8172726
    if (IS_JAVA_1_8) {
      prestartCoreThreads(commonPool(), getCommonPoolParallelism());
    }

    doStart(preStartThreads);

    scheduledExecutor = new ScheduledThreadPoolExecutor(1, new SchedulerThreadFactory(timerGroup, "%s"));
    scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    scheduledExecutor.setRemoveOnCancelPolicy(true);

    cronSchedulerHandler = new CronSchedulerHandler(parentGroup, threadPoolsConfig.getThreadNamePrefix());
    try {
      quartzScheduler = cronSchedulerHandler.getScheduler();
      quartzScheduler.start();
    } catch (SchedulerException | InterruptedException e) {
      throw new LifecycleException(e, this);
    }
  }

  protected abstract void doStart(boolean preStartThreads) throws MuleException;

  /**
   * Create queue using a {@link SynchronousQueue} if size is 0 or a {@link LinkedBlockingQueue} if size > 0.
   *
   * @param size queue size
   * @return new queue instance
   */
  protected BlockingQueue<Runnable> createQueue(int size) {
    return size == 0 ? new SynchronousQueue<>() : new LinkedBlockingQueue<>(size);
  }

  protected abstract void shutdownPools() throws MuleException, InterruptedException;

  public final void stop() throws MuleException, InterruptedException {
    shutdownPools();

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
    waitForExecutorTermination(startMillis);

    // When graceful shutdown timeouts, forceful shutdown will remove the custom scheduler from the list.
    // In that case, not creating a new collection here will cause a ConcurrentModificationException.
    for (ThreadPoolExecutor customSchedulerExecutor : new ArrayList<>(customSchedulersExecutors)) {
      waitForExecutorTermination(startMillis, customSchedulerExecutor,
                                 ((SchedulerThreadFactory) customSchedulerExecutor.getThreadFactory()).getGroup().getName());
    }

    onStopCompleted();
    scheduledExecutor = null;
    quartzScheduler = null;
  }

  protected abstract void waitForExecutorTermination(long shutdownStartMillis) throws InterruptedException;

  protected abstract void onStopCompleted();

  protected void waitForExecutorTermination(final long startMillis, final ExecutorService executor, final String executorLabel)
      throws InterruptedException {
    if (!executor
        .awaitTermination(threadPoolsConfig.getGracefulShutdownTimeout().getAsLong() - (currentTimeMillis() - startMillis),
                          MILLISECONDS)) {
      final List<Runnable> cancelledJobs = executor.shutdownNow();
      LOGGER.warn("'" + executorLabel + "' " + executor.toString() + " did not shutdown gracefully after "
          + threadPoolsConfig.getGracefulShutdownTimeout() + " milliseconds.");

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("The jobs " + cancelledJobs + " were cancelled.");
      } else {
        LOGGER.info(cancelledJobs.size() + " jobs were cancelled.");
      }
    }
  }

  public Scheduler createCpuLightScheduler(SchedulerConfig config, int parallelTasksEstimate,
                                           Supplier<Long> stopTimeout) {
    return createCpuLightScheduler(config, parallelTasksEstimate, stopTimeout, null);
  }

  public abstract Scheduler createCpuLightScheduler(SchedulerConfig config, int parallelTasksEstimate,
                                                    Supplier<Long> stopTimeout, ProfilingService profilingService);

  public Scheduler createIoScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout) {
    return createIoScheduler(config, workers, stopTimeout, null);
  }

  public abstract Scheduler createIoScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout,
                                              ProfilingService profilingService);

  protected boolean addScheduler(List<Scheduler> activeSchedulers, Scheduler scheduler) {
    activeSchedulersWriteLock.lock();
    try {
      return activeSchedulers.add(scheduler);
    } finally {
      activeSchedulersWriteLock.unlock();
    }
  }

  public Scheduler createCpuIntensiveScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout) {
    return createCpuIntensiveScheduler(config, workers, stopTimeout, null);
  }

  public abstract Scheduler createCpuIntensiveScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout,
                                                        ProfilingService profilingService);

  protected Consumer<Scheduler> shutdownCallback(List<Scheduler> activeSchedulers) {
    return schr -> {
      activeSchedulersWriteLock.lock();
      try {
        activeSchedulers.remove(schr);
      } finally {
        activeSchedulersWriteLock.unlock();
      }
    };
  }

  protected void validateCustomSchedulerOnlyConfigNotChanged(SchedulerConfig config) {
    if (config.getWaitAllowed().isPresent()) {
      throw new IllegalArgumentException("Only custom schedulers may define 'waitAllowed' behaviour");
    }
    if (config.getDirectRunCpuLightWhenTargetBusy().isPresent()) {
      throw new IllegalArgumentException("Only custom schedulers may define 'directRunCpuLightWhenTargetBusy' behaviour");
    }
  }

  protected boolean shouldThrottle(SchedulerConfig config, OptionalInt backingPoolMaxSize) {
    // Only throttle if max concurrent tasks is less than total backing pool size
    return config.getMaxConcurrentTasks() != null && config.getMaxConcurrentTasks() < backingPoolMaxSize.orElse(MAX_VALUE);
  }

  public Scheduler createCustomScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout) {
    return doCreateCustomScheduler(config, workers, stopTimeout, config.getMaxConcurrentTasks(), null);
  }

  public Scheduler createCustomScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout,
                                         ProfilingService profilingService) {
    return doCreateCustomScheduler(config, workers, stopTimeout, config.getMaxConcurrentTasks(), profilingService);
  }

  public Scheduler createCustomScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout, int queueSize) {
    return doCreateCustomScheduler(config, workers, stopTimeout, queueSize, null);
  }

  public Scheduler createCustomScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout, int queueSize,
                                         ProfilingService profilingService) {
    return doCreateCustomScheduler(config, workers, stopTimeout, queueSize, profilingService);
  }

  private Scheduler doCreateCustomScheduler(SchedulerConfig config, int workers, Supplier<Long> stopTimeout,
                                            int workQueueSize, ProfilingService profilingService) {
    String schedulerName = resolveCustomSchedulerName(config);
    String threadsName = resolveCustomThreadsName(config);
    BlockingQueue<Runnable> workQueue = createQueue(workQueueSize);
    if (config.getMaxConcurrentTasks() == null) {
      throw new IllegalArgumentException(
                                         "Custom schedulers must define a thread pool size bi calling `config.withMaxConcurrentTasks()`");
    }

    final ThreadGroup customChildGroup = new ThreadGroup(resolveThreadGroupForCustomScheduler(config), threadsName);
    final ThreadPoolExecutor executor =
        new ThreadPoolExecutor(config.getMaxConcurrentTasks(), config.getMaxConcurrentTasks(), 0L, MILLISECONDS, workQueue,
                               new SchedulerThreadFactory(customChildGroup, "%s.%02d"),
                               byCallerThreadGroupPolicy.apply(customChildGroup.getName()));

    prestartCoreThreads(executor, config.getMaxConcurrentTasks());

    Set<ThreadPoolExecutor> executors = customSchedulersExecutors;

    final CustomScheduler customScheduler =
        new CustomScheduler(schedulerName, executor, customChildGroup, workers, scheduledExecutor, quartzScheduler,
                            getCustomSchedulerDestroyerExecutor(), CUSTOM, stopTimeout,
                            shutdownCallback(activeCustomSchedulers).andThen(s -> executors.remove(executor)), profilingService);
    executors.add(executor);
    addScheduler(activeCustomSchedulers, customScheduler);
    return customScheduler;
  }

  protected abstract ThreadPoolExecutor getCustomSchedulerDestroyerExecutor();

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
   * @param executor     the executor to prestart the threads for
   * @param corePoolSize the number of threads to start in e=the {@code executor}
   */
  protected void prestartCoreThreads(final AbstractExecutorService executor, int corePoolSize) {
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
          prestartLatch.countDown();
          preStartCallback.accept(executor);
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

  private String resolveCustomSchedulerName(SchedulerConfig config) {
    return resolveSchedulerName(config, CUSTOM_THREADS_NAME);
  }

  protected String resolveSchedulerName(SchedulerConfig config, String prefix) {
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

  public abstract boolean isCurrentThreadForCpuWork();

  private static class CustomScheduler extends DefaultScheduler {

    private static final float THREADS_IN_GROUP_SIZE_MARGIN = 1.5f;

    private final ExecutorService executor;
    private final ThreadGroup threadGroup;

    private final ThreadPoolExecutor groupDestroyerExecutor;

    private CustomScheduler(String name, ExecutorService executor, ThreadGroup threadGroup, int workers,
                            ScheduledExecutorService scheduledExecutor, org.quartz.Scheduler quartzScheduler,
                            ThreadPoolExecutor groupDestroyerExecutor,
                            ThreadType threadsType, Supplier<Long> shutdownTimeoutMillis, Consumer<Scheduler> shutdownCallback,
                            ProfilingService profilingService) {
      super(name, executor, workers, scheduledExecutor, quartzScheduler, threadsType, shutdownTimeoutMillis,
            shutdownCallback, profilingService);
      this.executor = executor;
      this.threadGroup = threadGroup;
      this.groupDestroyerExecutor = groupDestroyerExecutor;
    }

    @Override
    public void shutdown() {
      LOGGER.debug("Shutting down " + this.toString());
      doShutdown();
      executor.shutdown();
      shutdownCallback.accept(this);
    }

    @Override
    public List<Runnable> shutdownNow() {
      LOGGER.debug("Shutting down NOW " + this.toString());
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

          if (LOGGER.isDebugEnabled()) {
            final StackTraceElement[] stackTrace = thread.getStackTrace();
            for (int i = 1; i < stackTrace.length; i++) {
              threadNamesBuilder.append("\t\tat ").append(stackTrace[i]).append(lineSeparator());
            }
          }
        }

        LOGGER.error("Unable to destroy ThreadGroup '{}' of Scheduler '{}' ({}). Remaining threads in the group are:"
            + lineSeparator() + "{}", threadGroup.getName(), this.getName(), destroyException.toString(),
                     threadNamesBuilder);
      }
    }

    private IllegalThreadStateException doDestroyThreadGroup() {
      IllegalThreadStateException destroyException = null;

      final long durationMillis = shutdownTimeoutMillis.get();
      final long stopNanos = nanoTime() + MILLISECONDS.toNanos(durationMillis) + SECONDS.toNanos(1);
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
      List<Scheduler> schedulers = new LinkedList<>(getOwnSchedulers());
      schedulers.addAll(activeCustomSchedulers);

      return schedulers;
    } finally {
      activeSchedulersReadLock.unlock();
    }
  }

  protected abstract List<Scheduler> getOwnSchedulers();

  public abstract String buildReportString();
}
