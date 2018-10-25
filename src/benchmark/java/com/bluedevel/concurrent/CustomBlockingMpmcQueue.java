/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.bluedevel.concurrent;

import org.jctools.queues.MpmcArrayQueue;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/*
 * A BlockingQueue implementation built upon Nitsan W's JCTools queues.
 *
 * Use it in a contended executor service, not much else is supported.
 */
public class CustomBlockingMpmcQueue<E> extends MpmcArrayQueue<E> implements BlockingQueue<E> {

  public CustomBlockingMpmcQueue(int capacity) {
    super(capacity);
  }

  @Override
  public void put(E e) throws InterruptedException {
    while (!offer(e)) {
      Thread.sleep(10);
    }
  }

  @Override
  public E take() throws InterruptedException {
    while (true) {
      E e = poll();

      if (e != null)
        return e;

      Thread.sleep(10);
    }
  }

  @Override
  public int drainTo(Collection<? super E> c) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int drainTo(Collection<? super E> c, int maxElements) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public E poll(long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int remainingCapacity() {
    throw new UnsupportedOperationException("not implemented");
  }

}
