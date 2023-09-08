/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.time.Duration.ofMillis;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SchedulerServiceStory.TASK_SCHEDULING;
import static reactor.core.publisher.Mono.just;
import static reactor.core.scheduler.Schedulers.fromExecutor;

import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import io.qameta.allure.Description;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;

@Feature(SCHEDULER_SERVICE)
@Story(TASK_SCHEDULING)
public class ReactorSchedulersTestCase extends AbstractMuleVsJavaExecutorTestCase {

  public ReactorSchedulersTestCase(Function<AbstractMuleVsJavaExecutorTestCase, ScheduledExecutorService> executorFactory,
                                   BlockingQueue<Runnable> sharedExecutorQueue, String param) {
    super(executorFactory, sharedExecutorQueue, param);
  }

  @Test
  @Description("Tests that timeout tasks scheduled and cancelled by reactor are not kept referenced in the scheduler")
  public void monoWithTimeout() {
    AtomicBoolean consumed = new AtomicBoolean(false);

    just("D'oh!").timeout(ofMillis(1000), just("timeout"), fromExecutor(executor)).subscribe(s -> {
      consumed.set(true);
      assertThat(s, is("D'oh!"));
    });

    assertThat(consumed.get(), is(true));

    assertThat(executor.shutdownNow(), empty());

  }

}
