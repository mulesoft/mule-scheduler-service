/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.mule.runtime.api.profiling.ProfilingService;
import org.slf4j.Logger;

/**
 * Decorates a {@link RunnableFuture} in order to do hook behavior before the execution of the decorated {@link RunnableFuture} so
 * a consistent state is maintained in the owner {@link DefaultScheduler}.
 *
 * @since 1.0
 */
class RunnableRepeatableFutureDecorator<V> extends AbstractRunnableFutureDecorator<V> implements RepeatableTaskWrapper {

  private static final Logger logger = getLogger(RunnableRepeatableFutureDecorator.class);

  private Supplier<RunnableFuture<V>> taskSupplier;
  private Runnable command;
  private final Consumer<RunnableRepeatableFutureDecorator<V>> wrapUpCallback;

  private final DefaultScheduler scheduler;

  private final String taskAsString;

  private volatile boolean running = false;
  private volatile boolean cancelled = false;
  private RunnableFuture<V> task;

  /**
   * Decorates the given {@code task}
   *
   * @param taskSupplier   the supplier for tasks to be decorated
   * @param command        the actual command that was given to the Scheduler
   * @param wrapUpCallback the callback to execute after the task is done
   * @param classLoader    the context {@link ClassLoader} on which the {@code task} should be executed
   * @param scheduler      the owner {@link Executor} of this task
   * @param taskAsString   a {@link String} representation of the task, used for logging and troubleshooting.
   * @param id             a unique it for this task.
   */
  RunnableRepeatableFutureDecorator(Supplier<RunnableFuture<V>> taskSupplier,
                                    Runnable command,
                                    Consumer<RunnableRepeatableFutureDecorator<V>> wrapUpCallback,
                                    ClassLoader classLoader, DefaultScheduler scheduler, String taskAsString, int id,
                                    ProfilingService profilingService) {
    super(id, classLoader, profilingService);
    this.taskSupplier = taskSupplier;
    this.command = command;
    this.wrapUpCallback = wrapUpCallback;
    this.scheduler = scheduler;
    this.taskAsString = taskAsString;
  }

  @Override
  public void run() {
    if (running) {
      if (logger.isTraceEnabled()) {
        logger.trace("{} still running, returning.", this);
      }
      return;
    }
    if (cancelled) {
      if (logger.isDebugEnabled()) {
        logger.debug("Task {} has been cancelled. Retunrning immendiately.", this);
      }
      return;
    }

    task = taskSupplier.get();

    doRun(task);
  }

  @Override
  protected long beforeRun() {
    running = true;
    return super.beforeRun();
  }

  @Override
  protected void wrapUp() throws Exception {
    running = false;
    try {
      wrapUpCallback.accept(this);
    } finally {
      super.wrapUp();
    }
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    this.cancelled = true;
    resetClassloader();
    boolean success = true;
    if (task != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Cancelling task {} (mayInterruptIfRunning={})...", this, mayInterruptIfRunning);
      }
      success = task.cancel(mayInterruptIfRunning);
    }
    scheduler.taskFinished(this);
    this.command = null;
    this.taskSupplier = null;
    return success;
  }

  @Override
  public boolean isCancelled() {
    return cancelled;
  }

  @Override
  public boolean isDone() {
    if (task != null) {
      return task.isDone();
    } else {
      return false;
    }
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    if (task != null) {
      return task.get();
    } else {
      return null;
    }
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    if (task != null) {
      return task.get(timeout, unit);
    } else {
      return null;
    }
  }

  @Override
  public Runnable getCommand() {
    return command;
  }

  @Override
  public String toString() {
    return getSchedulerName() + " - " + taskAsString + "(repeatable)";
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
