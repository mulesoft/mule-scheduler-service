/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static com.google.common.cache.CacheBuilder.newBuilder;
import static java.lang.Long.getLong;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.lineSeparator;
import static java.lang.Thread.currentThread;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.mule.runtime.api.scheduler.SchedulerConfig.config;
import static org.mule.runtime.api.scheduler.SchedulerContainerPoolsConfig.getInstance;
import static org.mule.runtime.core.api.config.MuleProperties.OBJECT_SCHEDULER_BASE_CONFIG;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.loadThreadPoolsConfig;
import static org.slf4j.LoggerFactory.getLogger;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.exception.MuleRuntimeException;
import org.mule.runtime.api.lifecycle.Startable;
import org.mule.runtime.api.lifecycle.Stoppable;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerConfig;
import org.mule.runtime.api.scheduler.SchedulerPoolsConfigFactory;
import org.mule.runtime.api.scheduler.SchedulerService;
import org.mule.runtime.api.scheduler.SchedulerView;
import org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig;
import org.mule.service.scheduler.internal.reporting.DefaultSchedulerView;
import org.mule.service.scheduler.internal.threads.SchedulerThreadPools;

import org.slf4j.Logger;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * Default implementation of {@link SchedulerService}.
 *
 * @since 1.0
 */
public class DefaultSchedulerService implements SchedulerService, Startable, Stoppable {

  private static final String USAGE_TRACE_INTERVAL_SECS_PROPERTY = "mule.scheduler.usageTraceIntervalSecs";
  public static final Long USAGE_TRACE_INTERVAL_SECS = getLong(USAGE_TRACE_INTERVAL_SECS_PROPERTY);

  private static final Logger logger = getLogger(DefaultSchedulerService.class);
  public static final Logger traceLogger = getLogger("org.mule.service.scheduler.trace");

  private static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 5000;
  private static final int CORES = getRuntime().availableProcessors();

  private ReadWriteLock pollsLock = new ReentrantReadWriteLock();
  private Lock pollsReadLock = pollsLock.readLock();
  private Lock pollsWriteLock = pollsLock.writeLock();

  private ContainerThreadPoolsConfig containerThreadPoolsConfig;
  private LoadingCache<SchedulerPoolsConfigFactory, SchedulerThreadPools> poolsByConfig;
  private Scheduler poolsMaintenanceScheduler;
  private ScheduledFuture<?> poolsMaintenanceTask;
  private ScheduledFuture<?> usageReportingTask;
  private volatile boolean started = false;
  private com.github.benmanes.caffeine.cache.LoadingCache<Thread, Boolean> cpuWorkCache = Caffeine.newBuilder().weakKeys()
      .build(this::cacheLoader);

  @Override
  public String getName() {
    return SchedulerService.class.getSimpleName();
  }

  @Override
  public Scheduler cpuLightScheduler() {
    checkStarted();
    final SchedulerConfig config = config();
    pollsReadLock.lock();
    try {
      return poolsByConfig.get(getInstance())
          .createCpuLightScheduler(config, cpuBoundWorkers(), resolveStopTimeout(config));
    } catch (ExecutionException e) {
      throw new MuleRuntimeException(e.getCause());
    } finally {
      pollsReadLock.unlock();
    }
  }

  @Override
  public Scheduler ioScheduler() {
    checkStarted();
    final SchedulerConfig config = config();
    pollsReadLock.lock();
    try {
      return poolsByConfig.get(getInstance())
          .createIoScheduler(config, ioBoundWorkers(), resolveStopTimeout(config));
    } catch (ExecutionException e) {
      throw new MuleRuntimeException(e.getCause());
    } finally {
      pollsReadLock.unlock();
    }
  }

  @Override
  public Scheduler cpuIntensiveScheduler() {
    checkStarted();
    final SchedulerConfig config = config();
    pollsReadLock.lock();
    try {
      return poolsByConfig.get(getInstance())
          .createCpuIntensiveScheduler(config, cpuBoundWorkers(), resolveStopTimeout(config));
    } catch (ExecutionException e) {
      throw new MuleRuntimeException(e.getCause());
    } finally {
      pollsReadLock.unlock();
    }
  }

  @Override
  public Scheduler cpuLightScheduler(SchedulerConfig config) {
    checkStarted();
    pollsReadLock.lock();
    try {
      return poolsByConfig.get(getInstance())
          .createCpuLightScheduler(config, cpuBoundWorkers(), resolveStopTimeout(config));
    } catch (ExecutionException e) {
      throw new MuleRuntimeException(e.getCause());
    } finally {
      pollsReadLock.unlock();
    }
  }

  @Override
  public Scheduler ioScheduler(SchedulerConfig config) {
    checkStarted();
    pollsReadLock.lock();
    try {
      return poolsByConfig.get(getInstance())
          .createIoScheduler(config, ioBoundWorkers(), resolveStopTimeout(config));
    } catch (ExecutionException e) {
      throw new MuleRuntimeException(e.getCause());
    } finally {
      pollsReadLock.unlock();
    }
  }

  @Override
  public Scheduler cpuIntensiveScheduler(SchedulerConfig config) {
    checkStarted();
    pollsReadLock.lock();
    try {
      return poolsByConfig.get(getInstance())
          .createCpuIntensiveScheduler(config, cpuBoundWorkers(), resolveStopTimeout(config));
    } catch (ExecutionException e) {
      throw new MuleRuntimeException(e.getCause());
    } finally {
      pollsReadLock.unlock();
    }
  }

  @Override
  @Inject
  public Scheduler cpuLightScheduler(@Named(OBJECT_SCHEDULER_BASE_CONFIG) SchedulerConfig config,
                                     SchedulerPoolsConfigFactory poolsConfigFactory) {
    checkStarted();
    pollsReadLock.lock();
    try {
      return poolsByConfig.get(poolsConfigFactory)
          .createCpuLightScheduler(config, cpuBoundWorkers(), resolveStopTimeout(config));
    } catch (ExecutionException e) {
      throw new MuleRuntimeException(e.getCause());
    } finally {
      pollsReadLock.unlock();
    }
  }

  @Override
  @Inject
  public Scheduler ioScheduler(@Named(OBJECT_SCHEDULER_BASE_CONFIG) SchedulerConfig config,
                               SchedulerPoolsConfigFactory poolsConfigFactory) {
    checkStarted();
    pollsReadLock.lock();
    try {
      return poolsByConfig.get(poolsConfigFactory)
          .createIoScheduler(config, ioBoundWorkers(), resolveStopTimeout(config));
    } catch (ExecutionException e) {
      throw new MuleRuntimeException(e.getCause());
    } finally {
      pollsReadLock.unlock();
    }
  }

  @Override
  @Inject
  public Scheduler cpuIntensiveScheduler(@Named(OBJECT_SCHEDULER_BASE_CONFIG) SchedulerConfig config,
                                         SchedulerPoolsConfigFactory poolsConfigFactory) {
    checkStarted();
    pollsReadLock.lock();
    try {
      return poolsByConfig.get(poolsConfigFactory)
          .createCpuIntensiveScheduler(config, cpuBoundWorkers(), resolveStopTimeout(config));
    } catch (ExecutionException e) {
      throw new MuleRuntimeException(e.getCause());
    } finally {
      pollsReadLock.unlock();
    }
  }

  private int cpuBoundWorkers() {
    return 4 * CORES;
  }

  private int ioBoundWorkers() {
    return CORES * CORES;
  }

  @Override
  @Inject
  public Scheduler customScheduler(@Named(OBJECT_SCHEDULER_BASE_CONFIG) SchedulerConfig config) {
    checkStarted();
    pollsReadLock.lock();
    try {
      return poolsByConfig.get(getInstance())
          .createCustomScheduler(config, CORES, resolveStopTimeout(config));
    } catch (ExecutionException e) {
      throw new MuleRuntimeException(e.getCause());
    } finally {
      pollsReadLock.unlock();
    }
  }

  @Override
  @Inject
  public Scheduler customScheduler(@Named(OBJECT_SCHEDULER_BASE_CONFIG) SchedulerConfig config, int queueSize) {
    checkStarted();
    pollsReadLock.lock();
    try {
      return poolsByConfig.get(getInstance())
          .createCustomScheduler(config, CORES, resolveStopTimeout(config), queueSize);
    } catch (ExecutionException e) {
      throw new MuleRuntimeException(e.getCause());
    } finally {
      pollsReadLock.unlock();
    }
  }

  private Supplier<Long> resolveStopTimeout(SchedulerConfig config) {
    return () -> config.getShutdownTimeoutMillis().get() != null ? config.getShutdownTimeoutMillis().get()
        : DEFAULT_SHUTDOWN_TIMEOUT_MILLIS;
  }

  private void checkStarted() {
    if (!started) {
      throw new IllegalStateException("Service " + getName() + " is not started.");
    }
  }

  @Override
  public boolean isCurrentThreadForCpuWork() {
    return cpuWorkCache.get(currentThread());
  }

  private boolean cacheLoader(Thread t) {
    return isCurrentThreadForCpuWork(getInstance());
  }

  @Override
  @Inject
  public boolean isCurrentThreadForCpuWork(SchedulerPoolsConfigFactory poolsConfigFactory) {
    checkStarted();
    pollsReadLock.lock();
    try {
      return poolsByConfig.get(poolsConfigFactory).isCurrentThreadForCpuWork();
    } catch (ExecutionException e) {
      throw new MuleRuntimeException(e.getCause());
    } finally {
      pollsReadLock.unlock();
    }
  }

  @Override
  public void start() throws MuleException {
    pollsWriteLock.lock();
    try {
      containerThreadPoolsConfig = loadThreadPoolsConfig();
      poolsByConfig = newBuilder()
          .weakKeys()
          .removalListener(new RemovalListener<SchedulerPoolsConfigFactory, SchedulerThreadPools>() {

            @Override
            public void onRemoval(RemovalNotification<SchedulerPoolsConfigFactory, SchedulerThreadPools> notification) {
              try {
                notification.getValue().stop();
                logger.info("Stopped " + this.toString());
              } catch (InterruptedException e) {
                currentThread().interrupt();
                logger.warn("Stop of " + this.toString() + " interrupted", e);
              } catch (MuleException e) {
                throw new MuleRuntimeException(e);
              }
            }
          })
          .build(new CacheLoader<SchedulerPoolsConfigFactory, SchedulerThreadPools>() {

            @Override
            public SchedulerThreadPools load(SchedulerPoolsConfigFactory key) throws Exception {
              SchedulerThreadPools containerThreadPools =
                  new SchedulerThreadPools(getName(), key.getConfig().orElse(containerThreadPoolsConfig));
              containerThreadPools.start();

              return containerThreadPools;
            }
          });

      started = true;

      poolsMaintenanceScheduler = customScheduler(config().withName("Scheduler Maintenace").withMaxConcurrentTasks(1));
      poolsMaintenanceTask = poolsMaintenanceScheduler.scheduleAtFixedRate(() -> poolsByConfig.cleanUp(), 1, 1, MINUTES);

      if (USAGE_TRACE_INTERVAL_SECS != null) {
        traceLogger.info("Usage Trace enabled");
        usageReportingTask = poolsMaintenanceScheduler.scheduleAtFixedRate(() -> {
          traceLogger.warn("************************************************************************");
          traceLogger.warn("* Schedulers Usage Report                                              *");
          traceLogger.warn("************************************************************************");
          for (SchedulerThreadPools pool : getPools()) {
            traceLogger.warn(pool.buildReportString());
            traceLogger.warn("************************************************************************");
          }
        }, USAGE_TRACE_INTERVAL_SECS, USAGE_TRACE_INTERVAL_SECS, SECONDS);
      }

    } finally {
      pollsWriteLock.unlock();
    }
  }

  @Override
  public void stop() throws MuleException {
    logger.info("Stopping " + this.toString() + "...");
    pollsWriteLock.lock();
    try {
      started = false;

      if (usageReportingTask != null) {
        usageReportingTask.cancel(true);
      }

      poolsMaintenanceTask.cancel(true);
      poolsMaintenanceScheduler.stop();
      poolsByConfig.invalidateAll();
      poolsByConfig = null;
      containerThreadPoolsConfig = null;
    } finally {
      pollsWriteLock.unlock();
    }
  }

  @Override
  public List<SchedulerView> getSchedulers() {
    List<SchedulerView> schedulers = new ArrayList<>();

    for (SchedulerThreadPools schedulerThreadPools : getPools()) {
      schedulers.addAll(schedulerThreadPools.getSchedulers().stream().map(s -> new DefaultSchedulerView(s)).collect(toList()));
    }

    return unmodifiableList(schedulers);
  }

  public Collection<SchedulerThreadPools> getPools() {
    pollsReadLock.lock();
    try {
      poolsByConfig.cleanUp();
      return poolsByConfig.asMap().values();
    } finally {
      pollsReadLock.unlock();
    }
  }

  @Override
  public String getSplashMessage() {
    StringBuilder splashMessage = new StringBuilder();

    splashMessage.append("Resolved configuration values:").append(lineSeparator());
    splashMessage.append("" + lineSeparator());
    splashMessage.append("gracefulShutdownTimeout:       ")
        .append(containerThreadPoolsConfig.getGracefulShutdownTimeout().getAsLong() + " ms" + lineSeparator());
    splashMessage.append("cpuLight.threadPool.size:      ")
        .append(containerThreadPoolsConfig.getCpuLightPoolSize().getAsInt() + lineSeparator());
    splashMessage.append("cpuLight.workQueue.size:       ")
        .append(containerThreadPoolsConfig.getCpuLightQueueSize().getAsInt() + lineSeparator());
    splashMessage.append("io.threadPool.maxSize:         ")
        .append(containerThreadPoolsConfig.getIoMaxPoolSize().getAsInt() + lineSeparator());
    splashMessage.append("io.threadPool.threadKeepAlive: ")
        .append(containerThreadPoolsConfig.getIoKeepAlive().getAsLong() + " ms" + lineSeparator());
    splashMessage.append("cpuIntensive.threadPool.size:  ")
        .append(containerThreadPoolsConfig.getCpuIntensivePoolSize().getAsInt() + lineSeparator());
    splashMessage.append("cpuIntensive.workQueue.size:   ")
        .append(containerThreadPoolsConfig.getCpuIntensiveQueueSize().getAsInt() + lineSeparator());
    splashMessage.append("" + lineSeparator());
    splashMessage.append("These can be modified by editing 'conf/scheduler-pools.conf'" + lineSeparator());

    return splashMessage.toString();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
