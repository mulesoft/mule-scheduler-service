/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.queue;

import static java.lang.System.nanoTime;
import static java.lang.Thread.interrupted;
import static java.lang.Thread.yield;

import org.jctools.queues.MpmcArrayQueue;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A BlockingQueue implementation built upon Nitsan W's JCTools queues.
 * <p>
 * Use it in a contended executor service, not much else is supported.
 *
 * @since 1.1.7
 */
public class CustomBlockingYieldMpmcQueue<E> extends MpmcArrayQueue<E> implements BlockingQueue<E> {

  public CustomBlockingYieldMpmcQueue(int capacity) {
    super(capacity);
  }

  @Override
  public void put(E e) throws InterruptedException {
    while (!offer(e)) {
      if (interrupted()) {
        throw new InterruptedException();
      } else {
        yield();
      }
    }
  }

  @Override
  public E take() throws InterruptedException {
    while (true) {
      E e = poll();

      if (e != null) {
        return e;
      }

      if (interrupted()) {
        throw new InterruptedException();
      } else {
        yield();
      }
    }
  }

  @Override
  public int drainTo(Collection<? super E> c) {
    return drain(e -> c.add(e));
  }

  @Override
  public int drainTo(Collection<? super E> c, int maxElements) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
    final long startNanos = nanoTime();

    boolean offered = offer(e);
    while (nanoTime() - unit.toNanos(timeout) < startNanos && !offered) {
      if (interrupted()) {
        throw new InterruptedException();
      } else {
        yield();
      }
      offered = offer(e);
    }

    return offered;
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    final long startNanos = nanoTime();

    while (nanoTime() - unit.toNanos(timeout) < startNanos) {
      E e = poll();

      if (e != null) {
        return e;
      }

      if (interrupted()) {
        throw new InterruptedException();
      } else {
        yield();
      }
    }

    return null;
  }

  @Override
  public int remainingCapacity() {
    throw new UnsupportedOperationException("not implemented");
  }

}
