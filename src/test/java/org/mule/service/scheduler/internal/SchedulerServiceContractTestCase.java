/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toSet;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mule.runtime.api.scheduler.SchedulerConfig.config;
import static org.mule.tck.probe.PollingProber.DEFAULT_POLLING_INTERVAL;
import static org.mule.test.allure.AllureConstants.SchedulerServiceFeature.SCHEDULER_SERVICE;

import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.scheduler.Scheduler;
import org.mule.runtime.api.scheduler.SchedulerPoolsConfig;
import org.mule.runtime.api.scheduler.SchedulerPoolsConfigFactory;
import org.mule.runtime.api.scheduler.SchedulerView;
import org.mule.service.scheduler.internal.service.DefaultSchedulerService;
import org.mule.tck.junit4.AbstractMuleTestCase;
import org.mule.tck.probe.JUnitLambdaProbe;
import org.mule.tck.probe.PollingProber;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.qameta.allure.Feature;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@Feature(SCHEDULER_SERVICE)
public abstract class SchedulerServiceContractTestCase extends AbstractMuleTestCase {

  public static final String SCHEDULER_MAINTENANCE_THREAD_PREFIX = "CUSTOM - Scheduler Maintenance";
  public static final String CPU_LIGHT_UBER_THRAD_PREFIX = "IO - uber";
  protected DefaultSchedulerService service;

  @Before
  public void before() throws MuleException {
    service = new DefaultSchedulerService();
    service.start();
  }

  @After
  public void after() throws MuleException {
    service.stop();
  }

  @Test
  public void defaultNoConfig() {
    assertThat(service.getPools(), hasSize(1));
    assertThat(service.getSchedulers(), hasSize(1));
    assertThat(getSchedulersRepresentation(service), hasItem(startsWith(SCHEDULER_MAINTENANCE_THREAD_PREFIX)));

    service.cpuLightScheduler();
    assertThat(service.getPools(), hasSize(1));
    assertThat(service.getSchedulers(), hasSize(2));
    assertThat(getSchedulersRepresentation(service), hasItem(startsWith(CPU_LIGHT_UBER_THRAD_PREFIX)));
    service.cpuLightScheduler();
    assertThat(service.getPools(), hasSize(1));
    assertThat(service.getSchedulers(), hasSize(3));
    assertThat(getSchedulersRepresentation(service),
               hasItems(startsWith(CPU_LIGHT_UBER_THRAD_PREFIX), startsWith(CPU_LIGHT_UBER_THRAD_PREFIX)));
    assertThat(areSchedulersActive(service), is(true));
  }

  @Test
  public void testWaitGroups() throws ExecutionException, InterruptedException {
    assertThat(schedulesTasksInWaitGroup(service.cpuLightScheduler()), is(true));
    assertThat(schedulesTasksInWaitGroup(service.customScheduler(config().withMaxConcurrentTasks(10))), is(false));
  }

  private boolean schedulesTasksInWaitGroup(Scheduler scheduler) throws ExecutionException, InterruptedException {
    return scheduler.schedule(() -> service.isCurrentThreadInWaitGroup(), 0, MILLISECONDS).get();
  }

  @Test
  public void artifactConfig() {
    assertThat(service.getPools(), hasSize(1));
    assertThat(service.getSchedulers(), hasSize(1));
    assertThat(getSchedulersRepresentation(service), hasItem(startsWith(SCHEDULER_MAINTENANCE_THREAD_PREFIX)));

    final SchedulerPoolsConfigFactory configFactory = getMockConfigFactory();
    service.cpuLightScheduler(config(), configFactory);
    assertThat(service.getPools(), hasSize(2));
    assertThat(service.getSchedulers(), hasSize(2));
    assertThat(getSchedulersRepresentation(service), hasItem(startsWith(getCpuLightPrefix())));
    service.cpuLightScheduler(config(), configFactory);
    assertThat(service.getPools(), hasSize(2));
    assertThat(service.getSchedulers(), hasSize(3));
    assertThat(getSchedulersRepresentation(service), hasItems(startsWith(getCpuLightPrefix()), startsWith(getCpuLightPrefix())));
  }

  @Test
  public void addWithArtifactConfig() {
    assertThat(service.getPools(), hasSize(1));
    service.cpuLightScheduler();
    assertThat(service.getPools(), hasSize(1));
    assertThat(service.getSchedulers(), hasSize(2));
    assertThat(getSchedulersRepresentation(service), hasItem(startsWith(CPU_LIGHT_UBER_THRAD_PREFIX)));
    service.cpuLightScheduler(config(), getMockConfigFactory());
    assertThat(service.getPools(), hasSize(2));
    assertThat(service.getSchedulers(), hasSize(3));
    assertThat(getSchedulersRepresentation(service),
               hasItems(startsWith(CPU_LIGHT_UBER_THRAD_PREFIX), startsWith(getCpuLightPrefix())));
  }

  @Test
  public void artifactGarbageCollectedConfig() {
    assertThat(service.getPools(), hasSize(1));

    // We cannot use Mockito to create this object, because Mockito keeps hard reference to the mocks it creates.
    SchedulerPoolsConfigFactory config = () -> of(getMockConfig());
    PhantomReference<SchedulerPoolsConfigFactory> configFactoryRef = new PhantomReference<>(config, new ReferenceQueue<>());

    service.cpuLightScheduler(config(), config);
    assertThat(service.getPools(), hasSize(2));

    config = null;

    new PollingProber(10000, 500).check(new JUnitLambdaProbe(() -> {
      System.gc();
      assertThat(configFactoryRef.isEnqueued(), is(true));
      assertThat(service.getPools(), hasSize(1));
      return true;
    }));
  }

  @Test
  public void stoppedScheduler() {
    Scheduler scheduler = service.cpuLightScheduler();
    assertThat(service.getSchedulers(), hasSize(2));
    assertThat(getSchedulersRepresentation(service), hasItem(startsWith(CPU_LIGHT_UBER_THRAD_PREFIX)));
    scheduler.stop();
    assertThat(service.getSchedulers(), hasSize(1));
  }

  protected abstract String getCpuLightPrefix();

  private Set<String> getSchedulersRepresentation(DefaultSchedulerService service) {
    return service.getSchedulers().stream().map(SchedulerView::toString).collect(toSet());
  }

  private boolean areSchedulersActive(DefaultSchedulerService service) {
    return service.getSchedulers().stream().noneMatch(SchedulerView::isShutdown)
        && service.getSchedulers().stream().noneMatch(SchedulerView::isTerminated);
  }

  private SchedulerPoolsConfigFactory getMockConfigFactory() {
    final SchedulerPoolsConfigFactory configFactory = mock(SchedulerPoolsConfigFactory.class);
    final SchedulerPoolsConfig config = getMockConfig();
    when(configFactory.getConfig()).thenReturn(of(config));
    return configFactory;
  }

  private SchedulerPoolsConfig getMockConfig() {
    final SchedulerPoolsConfig config = mock(SchedulerPoolsConfig.class);

    when(config.getGracefulShutdownTimeout()).thenReturn(OptionalLong.of(30000L));
    when(config.getThreadNamePrefix()).thenReturn("test");

    configure(config);

    return config;
  }

  protected abstract void configure(SchedulerPoolsConfig config);
}
