/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.queue;

import static java.lang.Integer.MAX_VALUE;

import org.mule.runtime.api.scheduler.SchedulerBusyException;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * {@link BlockingQueue} implementation that always returns {@code false} when {@link #offer(Object)} is called.
 * <p>
 * This is intended only to be used with a {@link ThreadPoolExecutor} along with the {@link RejectedExecutionHandler} returned by
 * {@link #buildHandler(RejectedExecutionHandler)}.
 *
 * @since 4.0
 */
public class AsyncHandOffQueue implements BlockingQueue<Runnable> {

  private LinkedBlockingQueue<Runnable> innerQueue = new LinkedBlockingQueue<>(MAX_VALUE);

  public RejectedExecutionHandler buildHandler(RejectedExecutionHandler delegateRejectedExecutionHandler) {
    return (r, executor) -> {
      try {
        delegateRejectedExecutionHandler.rejectedExecution(r, executor);
      } catch (SchedulerBusyException e) {
        // Enqueue anyway, so that the newly created worker takes this task
        if (!innerQueue.offer(r)) {
          throw e;
        }
      }
    };
  }

  @Override
  public boolean offer(Runnable e) {
    // Force creation of a worker.
    // This is so based on the implementation of ThreadPoolExecutor#execute(Runnable)
    return false;
  }

  @Override
  public boolean offer(Runnable e, long timeout, TimeUnit unit) throws InterruptedException {
    // Force creation of a worker.
    // This is so based on the implementation of ThreadPoolExecutor#execute(Runnable)
    return false;
  }

  @Override
  public Runnable remove() {
    return innerQueue.remove();
  }

  @Override
  public Runnable poll() {
    return innerQueue.poll();
  }

  @Override
  public Runnable element() {
    return innerQueue.element();
  }

  @Override
  public Runnable peek() {
    return innerQueue.element();
  }

  @Override
  public int size() {
    return innerQueue.size();
  }

  @Override
  public boolean isEmpty() {
    return innerQueue.isEmpty();
  }

  @Override
  public Iterator<Runnable> iterator() {
    return innerQueue.iterator();
  }

  @Override
  public Object[] toArray() {
    return innerQueue.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return innerQueue.toArray(a);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return innerQueue.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends Runnable> c) {
    return innerQueue.addAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return innerQueue.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return innerQueue.retainAll(c);
  }

  @Override
  public void clear() {
    innerQueue.clear();
  }

  @Override
  public boolean add(Runnable e) {
    return innerQueue.add(e);
  }

  @Override
  public void put(Runnable e) throws InterruptedException {
    innerQueue.put(e);
  }

  @Override
  public Runnable take() throws InterruptedException {
    return innerQueue.take();
  }

  @Override
  public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
    return innerQueue.poll(timeout, unit);
  }

  @Override
  public int remainingCapacity() {
    return innerQueue.remainingCapacity();
  }

  @Override
  public boolean remove(Object o) {
    return innerQueue.remove(o);
  }

  @Override
  public boolean contains(Object o) {
    return innerQueue.contains(o);
  }

  @Override
  public int drainTo(Collection<? super Runnable> c) {
    return innerQueue.drainTo(c);
  }

  @Override
  public int drainTo(Collection<? super Runnable> c, int maxElements) {
    return innerQueue.drainTo(c, maxElements);
  }

}
