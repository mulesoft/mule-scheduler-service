/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static org.mule.runtime.api.profiling.type.RuntimeProfilingEventTypes.SCHEDULING_TASK_EXECUTION;
import static org.mule.runtime.api.profiling.type.RuntimeProfilingEventTypes.STARTING_TASK_EXECUTION;
import static org.mule.runtime.api.profiling.type.RuntimeProfilingEventTypes.TASK_EXECUTED;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

import org.mule.runtime.api.profiling.ProfilingService;
import org.mule.runtime.api.exception.MuleRuntimeException;
import org.mule.runtime.api.profiling.tracing.ExecutionContext;
import org.mule.service.scheduler.internal.profiling.DefaultTaskSchedulingProfilingEventContext;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;

import org.slf4j.Logger;
import org.slf4j.MDC;

/**
 * Abstract base decorator for a a {@link RunnableFuture} in order to do hook behavior before the execution of the decorated
 * {@link RunnableFuture} so a consistent state is maintained in the owner {@link DefaultScheduler}.
 *
 * @since 1.0
 */
abstract class AbstractRunnableFutureDecorator<V> implements RunnableFuture<V> {

  private static final Logger logger = getLogger(AbstractRunnableFutureDecorator.class);

  private ClassLoader classLoader;

  private Thread runningThread;

  /**
   * The remembered value of the thread-locals before the execution of the task. We use this so the task can not interfere with
   * the thread-locals of the caller thread for cases in which the task is executed on the same thread. Note that this can be null
   * if there was nothing yet on the thread's thread-locals.
   */
  private Object previousThreadLocals;

  /**
   * Reference to the threadLocals field of the {@link Thread} class, this is necessary because it is not accessible by default.
   */
  private static final Field threadLocalsField;

  static {
    try {
      // Grabs a reference to the threadLocals field and sets it as accessible using reflection.
      threadLocalsField = Thread.class.getDeclaredField("threadLocals");
      threadLocalsField.setAccessible(true);
    } catch (NoSuchFieldException | SecurityException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * In order to provide thread-locals isolation for the current task in case it is executed on the same thread following a
   * rejection policy, we remember the current reference and set the internal attribute to null to force re-creation as if it was
   * a new thread.
   *
   * See also {@link #restorePreviousThreadLocals()}.
   */
  private void rememberAndClearCurrentThreadLocals() {
    try {
      // Remembers the reference to the current thread-locals map.
      previousThreadLocals = threadLocalsField.get(currentThread());
      // Clears the current thread-locals, so they can be recreated lazily by the current task.
      threadLocalsField.set(currentThread(), null);
    } catch (IllegalAccessException e) {
      throw new MuleRuntimeException(e);
    }
  }

  /**
   * Restores the current thread's thread-locals to the value they had before {@link #rememberAndClearCurrentThreadLocals()}.
   */
  private void restorePreviousThreadLocals() {
    try {
      // By restoring the thread-locals map reference to the previous one, we are releasing all current thread-locals, so
      // they can be reclaimed.
      threadLocalsField.set(currentThread(), previousThreadLocals);
      previousThreadLocals = null;
    } catch (Exception e) {
      throw new MuleRuntimeException(e);
    }
  }

  private final int id;
  private volatile boolean ranAtLeastOnce = false;
  private volatile boolean started = false;
  private ProfilingService profilingService = null;
  private ExecutionContext initialExecutionContext = null;

  /**
   * @param id          a unique it for this task.
   * @param classLoader the context {@link ClassLoader} on which the {@code task} should be executed
   */
  protected AbstractRunnableFutureDecorator(int id, ClassLoader classLoader, ProfilingService profilingService) {
    this.id = id;
    this.classLoader = classLoader;
    // At this point, the thread is the one that scheduled the task execution. We store its context to allow its propagation.
    if (shouldProfile(profilingService)) {
      this.profilingService = profilingService;
      initialExecutionContext = profilingService.getTracingService().getCurrentExecutionContext();
      profilingService.getProfilingDataProducer(SCHEDULING_TASK_EXECUTION)
          .triggerProfilingEvent(new DefaultTaskSchedulingProfilingEventContext(currentTimeMillis(), valueOf(id),
                                                                                currentThread().getName(),
                                                                                initialExecutionContext));
    }
  }

  protected long beforeRun() {
    long startTime = 0;
    if (logger.isTraceEnabled()) {
      startTime = nanoTime();
      logger.trace("Starting task " + this + "...");
    }
    ranAtLeastOnce = true;
    started = true;

    rememberAndClearCurrentThreadLocals();

    return startTime;
  }

  /**
   * Performs the required bookkeeping before and after running the task, as well as setting the appropriate context for the task.
   * <p>
   * Any {@link Exception} thrown as part of the task processing or bookkeeping is handled by this method and not rethrown.
   *
   * @param task the task to run
   */
  protected void doRun(RunnableFuture<V> task) {
    ClassLoader cl = classLoader;

    if (cl == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Task " + this + " has been cancelled. Returning immediately.");
      }
      return;
    }

    long startTime = beforeRun();

    final Thread currentThread = currentThread();
    final ClassLoader currentClassLoader = currentThread.getContextClassLoader();
    final String currentThreadName = currentThread.getName();

    currentThread.setContextClassLoader(cl);
    if (getThreadNameSuffix() != null) {
      currentThread.setName(currentThreadName.concat(": ").concat(getThreadNameSuffix()));
    }
    this.runningThread = currentThread;
    if (logger.isTraceEnabled()) {
      MDC.put("task", task.toString());
    }

    try {
      if (shouldProfile(profilingService)) {
        if (initialExecutionContext != null) {
          profilingService.getTracingService().setCurrentExecutionContext(initialExecutionContext);
        }
        profilingService.getProfilingDataProducer(STARTING_TASK_EXECUTION)
            .triggerProfilingEvent(new DefaultTaskSchedulingProfilingEventContext(currentTimeMillis(), valueOf(id),
                                                                                  currentThread().getName(),
                                                                                  profilingService
                                                                                      .getTracingService()
                                                                                      .getCurrentExecutionContext()));
      }
      task.run();
      if (task.isCancelled()) {
        if (logger.isTraceEnabled()) {
          // Log instead of rethrow to avoid flooding the logger with stack traces of cancellation, which may be very common.
          logger.trace("Task " + this + " cancelled");
        }
      } else {
        task.get();
      }
    } catch (ExecutionException e) {
      logger.error("Uncaught throwable in task " + this, e);
    } catch (InterruptedException e) {
      currentThread.interrupt();
    } finally {
      try {
        wrapUp();
      } catch (Exception e) {
        logger.error("Exception wrapping up execution of " + this, e);
      } finally {
        if (logger.isTraceEnabled()) {
          logger.trace("Task " + this + " finished after " + (nanoTime() - startTime) + " nanoseconds");
        }

        currentThread.setContextClassLoader(currentClassLoader);
        if (getThreadNameSuffix() != null) {
          currentThread.setName(currentThreadName);
        }
      }
    }
  }

  protected void resetClassloader() {
    // Since this object may be alive until the next time this task is triggered, we are eager to release the classloader.
    this.classLoader = null;
  }

  protected void wrapUp() throws Exception {
    if (shouldProfile(profilingService)) {
      profilingService.getProfilingDataProducer(TASK_EXECUTED)
          .triggerProfilingEvent(new DefaultTaskSchedulingProfilingEventContext(currentTimeMillis(), valueOf(id),
                                                                                currentThread().getName(), profilingService
                                                                                    .getTracingService()
                                                                                    .getCurrentExecutionContext()));
      profilingService.getTracingService().deleteCurrentExecutionContext();
    }
    started = false;
    runningThread = null;
    restorePreviousThreadLocals();
  }

  /**
   * @return {@code true} if the execution of this task was started at least once, false otherwise.
   */
  boolean isRanAtLeastOnce() {
    return ranAtLeastOnce;
  }

  /**
   * @return {@code true} if the execution of this task has already started, false otherwise.
   */
  boolean isStarted() {
    return started;
  }

  /**
   * @return the thread where this task is running, or {@code null} if it not running.
   */
  public Thread getRunningThread() {
    return runningThread;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(id);
  }

  public abstract String getSchedulerName();

  public abstract String getThreadNameSuffix();

  private boolean shouldProfile(ProfilingService profilingService) {
    return profilingService != null;
  }
}
