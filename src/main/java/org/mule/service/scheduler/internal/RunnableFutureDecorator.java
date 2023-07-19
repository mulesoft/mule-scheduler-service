/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 */
package org.mule.service.scheduler.internal;

import static org.slf4j.LoggerFactory.getLogger;

import org.mule.runtime.api.profiling.ProfilingService;
import org.slf4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Decorates a {@link RunnableFuture} in order to do hook behavior both before and after the execution of the decorated
 * {@link RunnableFuture} so a consistent state is maintained in the owner {@link DefaultScheduler}.
 *
 * @since 1.0
 */
class RunnableFutureDecorator<V> extends AbstractRunnableFutureDecorator<V> {

  private static final Logger logger = getLogger(RunnableFutureDecorator.class);

  private final RunnableFuture<V> task;

  private final DefaultScheduler scheduler;

  private final String taskAsString;

  /**
   * Decorates the given {@code task}
   *
   * @param task         the task to be decorated
   * @param classLoader  the context {@link ClassLoader} on which the {@code task} should be executed
   * @param scheduler    the owner {@link Executor} of this task
   * @param taskAsString a {@link String} representation of the task, used for logging and troubleshooting.
   * @param id           a unique it for this task.
   */
  RunnableFutureDecorator(RunnableFuture<V> task, ClassLoader classLoader, DefaultScheduler scheduler, String taskAsString,
                          int id, ProfilingService profilingService) {
    super(id, classLoader, profilingService);
    this.task = task;
    this.scheduler = scheduler;
    this.taskAsString = taskAsString;
  }

  @Override
  public void run() {
    doRun(task);
  }

  @Override
  protected void wrapUp() throws Exception {
    scheduler.taskFinished(this);
    super.wrapUp();
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    resetClassloader();
    if (logger.isDebugEnabled()) {
      logger.debug("Cancelling task " + this.toString() + " (mayInterruptIfRunning=" + mayInterruptIfRunning + ")...");
    }
    boolean success = task.cancel(mayInterruptIfRunning);
    scheduler.taskFinished(this);
    return success;
  }

  @Override
  public boolean isCancelled() {
    return task.isCancelled();
  }

  @Override
  public boolean isDone() {
    return task.isDone();
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    return task.get();
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return task.get(timeout, unit);
  }

  @Override
  public String toString() {
    return getSchedulerName() + " - " + taskAsString;
  }

  @Override
  public String getSchedulerName() {
    return scheduler.getName();
  }

  @Override
  public String getThreadNameSuffix() {
    return scheduler.getThreadNameSuffix();
  }
}
