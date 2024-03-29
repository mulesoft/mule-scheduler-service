/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.executor;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.Assert.assertFalse;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import org.mule.runtime.api.scheduler.SchedulerBusyException;
import org.mule.runtime.core.api.util.StringUtils;
import org.mule.tck.junit4.AbstractMuleTestCase;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class WaitPolicyTestCase extends AbstractMuleTestCase {

  private ExceptionCollectingThreadGroup threadGroup;
  ThreadPoolExecutor executor;
  ReentrantLock executorLock;

  @Before
  public void startExecutor() {
    // allow 1 active & 1 queued Thread
    executor = new ThreadPoolExecutor(1, 1, 10000L, MILLISECONDS, new LinkedBlockingQueue<Runnable>(1));
    executor.prestartAllCoreThreads();

    // the lock must be fair to guarantee FIFO access to the executor;
    // 'synchronized' on a monitor is not good enough.
    executorLock = new ReentrantLock(true);

    // this is a ThreadGroup that collects uncaught exceptions. Necessary for JDK
    // 1.4.x only.
    threadGroup = new ExceptionCollectingThreadGroup();

    // reset counter of active SleepyTasks
    SleepyTask.activeTasks.set(0);
  }

  @After
  public void shutDownExecutor() {
    executor.shutdown();
    threadGroup.destroy();
  }

  // Submit the given Runnables to an ExecutorService, but do so in separate
  // threads to avoid blocking the test case when the Executors queue is full.
  // Returns control to the caller when the threads have been started in
  // order to avoid OS-dependent delays in the control flow.
  // A submitting thread waits on a fair Lock to guarantee FIFO ordering.
  // At the time of return the Runnables may or may not be in the queue,
  // rejected, running or already finished. :-)
  protected LinkedList<Thread> execute(final List<Runnable> tasks) throws InterruptedException {
    if (tasks == null || tasks.isEmpty()) {
      throw new IllegalArgumentException("List<Runnable> must not be empty");
    }

    LinkedList<Thread> submitters = new LinkedList<>();

    executorLock.lock();

    for (Runnable task : tasks) {
      Runnable submitterAction = () -> {
        // the lock is important because otherwise two submitters might
        // stumble over each other, submitting their runnables out-of-order
        // and causing test failures.
        try {
          executorLock.lock();
          executor.execute(task);
        } finally {
          executorLock.unlock();
        }
      };

      Thread submitter = new Thread(threadGroup, submitterAction);
      submitter.setDaemon(true);
      submitters.add(submitter);
      submitter.start();

      // wait until the thread is actually enqueued on the lock
      while (submitter.isAlive() && !executorLock.hasQueuedThread(submitter)) {
        sleep(10);
      }
    }

    executorLock.unlock();
    return submitters;
  }

  @Test
  public void testWaitPolicyForever() throws Exception {
    assertEquals(0, SleepyTask.activeTasks.get());

    // tasks wait forever
    LastRejectedWaitPolicy policy = new LastRejectedWaitPolicy(-1, SECONDS);
    executor.setRejectedExecutionHandler(policy);

    // create tasks
    List<Runnable> tasks = new ArrayList<>();
    // task 1 runs immediately
    tasks.add(new SleepyTask("run", 1000));
    // task 2 is queued
    tasks.add(new SleepyTask("queued", 1000));
    // task 3 is initially rejected but waits forever
    Runnable waiting = new SleepyTask("waitingForever", 1000);
    tasks.add(waiting);

    // submit tasks
    LinkedList<Thread> submitters = this.execute(tasks);
    assertFalse(submitters.isEmpty());

    // the last task should have been queued
    assertFalse(executor.awaitTermination(4000, MILLISECONDS));
    assertSame(waiting, policy.lastRejectedRunnable());
    assertEquals(0, SleepyTask.activeTasks.get());
  }

  @Test
  public void testWaitPolicyWithTimeout() throws Exception {
    assertEquals(0, SleepyTask.activeTasks.get());

    // set a reasonable retry interval
    LastRejectedWaitPolicy policy = new LastRejectedWaitPolicy(2500, MILLISECONDS);
    executor.setRejectedExecutionHandler(policy);

    // create tasks
    List<Runnable> tasks = new ArrayList<>();
    // task 1 runs immediately
    tasks.add(new SleepyTask("run", 1000));
    // task 2 is queued
    tasks.add(new SleepyTask("queued", 1000));
    // task 3 is initially rejected but will eventually succeed
    Runnable waiting = new SleepyTask("waiting", 1000);
    tasks.add(waiting);

    // submit tasks
    LinkedList<Thread> submitters = this.execute(tasks);
    assertFalse(submitters.isEmpty());

    assertFalse(executor.awaitTermination(5000, MILLISECONDS));
    assertSame(waiting, policy.lastRejectedRunnable());
    assertEquals(0, SleepyTask.activeTasks.get());
  }

  @Test
  public void testWaitPolicyWithTimeoutFailure() throws Exception {
    assertEquals(0, SleepyTask.activeTasks.get());

    // set a really short wait interval
    long failureInterval = 100L;
    LastRejectedWaitPolicy policy = new LastRejectedWaitPolicy(failureInterval, MILLISECONDS);
    executor.setRejectedExecutionHandler(policy);

    // create tasks
    List<Runnable> tasks = new ArrayList<>();
    // task 1 runs immediately
    tasks.add(new SleepyTask("run", 1000));
    // task 2 is queued
    tasks.add(new SleepyTask("queued", 1000));
    // task 3 is initially rejected & will retry but should fail quickly
    Runnable failedTask = new SleepyTask("waitAndFail", 1000);
    tasks.add(failedTask);

    // submit tasks
    LinkedList<Thread> submitters = this.execute(tasks);
    assertFalse(submitters.isEmpty());

    // give failure a chance
    sleep(failureInterval * 10);

    // make sure there was one failure
    LinkedList<Map<Thread, Throwable>> exceptions = threadGroup.collectedExceptions();
    assertThat(exceptions, hasSize(1));

    // make sure the failed task was the right one
    Map.Entry<Thread, Throwable> threadFailure = exceptions.getFirst().entrySet().iterator().next();
    assertThat(threadFailure.getKey(), is(submitters.getLast()));
    assertThat(threadFailure.getValue(), instanceOf(SchedulerBusyException.class));

    executor.shutdown();
    assertThat(executor.awaitTermination(2500, MILLISECONDS), is(true));
    assertThat(policy.lastRejectedRunnable(), Matchers.sameInstance(failedTask));
    assertThat(SleepyTask.activeTasks.get(), is(0));
  }
}


class LastRejectedWaitPolicy extends WaitPolicy {

  // needed to hand the last rejected Runnable back to the TestCase
  private volatile Runnable _rejected;

  public LastRejectedWaitPolicy() {
    super((r, e) -> {
    }, "test");
  }

  public LastRejectedWaitPolicy(long time, TimeUnit timeUnit) {
    super(time, timeUnit, (r, e) -> {
    }, "test");
  }

  public Runnable lastRejectedRunnable() {
    return _rejected;
  }

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
    _rejected = r;
    super.rejectedExecution(r, e);
  }
}


// task to execute - just sleeps for the given interval
class SleepyTask extends Object implements Runnable {

  public static final AtomicInteger activeTasks = new AtomicInteger(0);

  private final String name;
  private final long sleepTime;

  public SleepyTask(String name, long sleepTime) {
    if (StringUtils.isEmpty(name)) {
      throw new IllegalArgumentException("SleepyTask needs a name!");
    }

    this.name = name;
    this.sleepTime = sleepTime;
  }

  @Override
  public String toString() {
    return this.getClass().getName() + '{' + name + ", " + sleepTime + '}';
  }

  @Override
  public void run() {
    activeTasks.incrementAndGet();

    try {
      sleep(sleepTime);
    } catch (InterruptedException iex) {
      currentThread().interrupt();
    } finally {
      activeTasks.decrementAndGet();
    }
  }

}


// ThreadGroup wrapper that collects uncaught exceptions
class ExceptionCollectingThreadGroup extends ThreadGroup {

  private final LinkedList<Map<Thread, Throwable>> exceptions = new LinkedList<>();

  public ExceptionCollectingThreadGroup() {
    super("ExceptionCollectingThreadGroup");
  }

  // collected Map(Thread, Throwable) associations
  public LinkedList<Map<Thread, Throwable>> collectedExceptions() {
    return exceptions;
  }

  // all uncaught Thread exceptions end up here
  @Override
  public void uncaughtException(Thread t, Throwable e) {
    synchronized (exceptions) {
      exceptions.add(singletonMap(t, e));
    }
  }
}
