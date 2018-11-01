/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.queue;

import static java.lang.System.nanoTime;
import static java.lang.Thread.interrupted;
import static java.lang.Thread.sleep;
import static java.lang.Thread.yield;

import org.jctools.queues.MpmcArrayQueue;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
      checkInterrupt(yieldingFromIn);
    }
    yieldingFromIn.set(-1);
  }

  @Override
  public E take() throws InterruptedException {
    while (true) {
      E e = poll();

      if (e != null) {
        yieldingFromOut.set(-1);
        return e;
      }

      checkInterrupt(yieldingFromOut);
    }
  }

  @Override
  public int drainTo(Collection<? super E> c) {
    return drain(e -> c.add(e));
  }

  @Override
  public int drainTo(Collection<? super E> c, int maxElements) {
    return drain(e -> c.add(e), maxElements);
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
    final long startNanos = nanoTime();

    boolean offered = offer(e);
    while (nanoTime() - unit.toNanos(timeout) < startNanos && !offered) {
      checkInterrupt(yieldingFromIn);
      offered = offer(e);
    }

    if (offered) {
      yieldingFromIn.set(-1);
    }
    return offered;
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    final long startNanos = nanoTime();

    while (nanoTime() - unit.toNanos(timeout) < startNanos) {
      E e = poll();

      if (e != null) {
        yieldingFromOut.set(-1);
        return e;
      }

      checkInterrupt(yieldingFromOut);
    }

    return null;
  }

  private AtomicLong yieldingFromIn = new AtomicLong(-1);
  private AtomicLong yieldingFromOut = new AtomicLong(-1);

  protected void checkInterrupt(AtomicLong yieldingFrom) throws InterruptedException {
    long now = System.currentTimeMillis();

    // long currentYieldingFrom = yieldingFrom.get();
    if (yieldingFrom.compareAndSet(-1, now)) {
      if (interrupted()) {
        throw new InterruptedException();
      }
      yield();
    } else if (yieldingFrom.get() > now - 1000) {
      if (interrupted()) {
        throw new InterruptedException();
      }
      yield();
    } else {
      sleep(10);
    }
  }

  @Override
  public int remainingCapacity() {
    throw new UnsupportedOperationException("not implemented");
  }

}
