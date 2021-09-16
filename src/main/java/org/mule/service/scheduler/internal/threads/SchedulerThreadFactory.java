/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.threads;

import org.mule.runtime.api.profiling.context.threading.ThreadProfilingContext;

import static java.lang.String.format;
import static java.security.AccessController.doPrivileged;
import static java.security.AccessController.getContext;
import static org.mule.runtime.core.api.util.ClassUtils.withContextClassLoader;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessControlContext;
import java.security.PrivilegedAction;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link ThreadFactory} implementation that sets a {@link ThreadGroup} and a name with a counter to the created {@link Thread}s
 *
 * @since 1.0
 */
public class SchedulerThreadFactory implements java.util.concurrent.ThreadFactory {

  private static final AccessControlContext ACCESS_CONTROL_CTX = getContext();

  private final ThreadGroup group;
  private final String nameFormat;
  private final AtomicLong counter;

  public SchedulerThreadFactory(ThreadGroup group) {
    this(group, "%s.%02d");
  }

  public SchedulerThreadFactory(ThreadGroup group, String nameFormat) {
    this.group = group;
    this.nameFormat = nameFormat;
    this.counter = new AtomicLong(1);
  }

  @Override
  public Thread newThread(Runnable runnable) {
    return withContextClassLoader(this.getClass().getClassLoader(), () -> {
      // Avoid the created thread to inherit the security context of the caller thread's stack.
      // If the thread creation is triggered by a deployable artifact classloader, a reference to it would be kept by the created
      // thread without this doPrivileged call.
      return addRuntimeContext(doPrivileged((PrivilegedAction<Thread>) () -> new Thread(group, runnable,
                                                                                        format(nameFormat, group.getName(),
                                                                                               counter.getAndIncrement())),
                                            ACCESS_CONTROL_CTX));
    });
  }

  public ThreadGroup getGroup() {
    return group;
  }

  public AtomicLong getCounter() {
    return counter;
  }

  private static Thread addRuntimeContext(Thread thread) {
    // TODO: Add a feature flag ever "thread traceability" or something like that (always false by default).
    try {
      Method createMap = ThreadLocal.class.getDeclaredMethod("createMap", Thread.class, Object.class);
      createMap.invoke(ThreadProfilingContext.getCurrentThreadProfilingContext(), thread, new ThreadProfilingContext());
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ignored) {
      // The affected functionality is non critical (profiling), so we just log the error.
      // TODO: Log the error
    }
    return thread;
  }
}
