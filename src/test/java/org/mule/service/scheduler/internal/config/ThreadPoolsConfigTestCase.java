/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.config;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.clearProperty;
import static java.lang.System.setProperty;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.DEDICATED;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.UBER;
import static org.mule.runtime.core.api.config.MuleProperties.MULE_HOME_DIRECTORY_PROPERTY;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.BIG_POOL_DEFAULT_SIZE;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.CPU_INTENSIVE_PREFIX;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.CPU_LIGHT_PREFIX;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.IO_PREFIX;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.PROP_PREFIX;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.SCHEDULER_POOLS_CONFIG_FILE_PROPERTY;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.STRATEGY_PROPERTY_NAME;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.THREAD_POOL_KEEP_ALIVE;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.THREAD_POOL_SIZE;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.THREAD_POOL_SIZE_CORE;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.THREAD_POOL_SIZE_MAX;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.UBER_QUEUE_SIZE;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.UBER_THREAD_POOL_KEEP_ALIVE;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.UBER_THREAD_POOL_SIZE_CORE;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.UBER_THREAD_POOL_SIZE_MAX;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.WORK_QUEUE_SIZE;
import static org.mule.service.scheduler.internal.config.ContainerThreadPoolsConfig.loadThreadPoolsConfig;

import org.mule.runtime.api.exception.DefaultMuleException;
import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.scheduler.SchedulerPoolsConfig;
import org.mule.tck.junit4.AbstractMuleTestCase;
import org.mule.tck.junit4.rule.SystemProperty;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import io.qameta.allure.Description;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class ThreadPoolsConfigTestCase extends AbstractMuleTestCase {

  private static int CORES = getRuntime().availableProcessors();
  private static long MEM = getRuntime().maxMemory() / 1024;

  @Rule
  public SystemProperty configFile = new SystemProperty(SCHEDULER_POOLS_CONFIG_FILE_PROPERTY, null);

  @Rule
  public SystemProperty configOverrideFile = new SystemProperty("org.mule.runtime.scheduler.io.threadPool.maxSize", null);

  @Rule
  public TemporaryFolder tempMuleHome = new TemporaryFolder();

  @Rule
  public TemporaryFolder tempOtherDir = new TemporaryFolder();

  @Rule
  public ExpectedException expected = ExpectedException.none();

  private File schedulerConfigFile;

  @Before
  public void before() {
    final File confDir = new File(tempMuleHome.getRoot(), "conf");
    confDir.mkdir();
    schedulerConfigFile = new File(confDir, "scheduler-pools.conf");
    setProperty(MULE_HOME_DIRECTORY_PROPERTY, tempMuleHome.getRoot().getAbsolutePath());
  }

  @After
  public void after() {
    clearProperty(MULE_HOME_DIRECTORY_PROPERTY);
  }

  protected Properties buildDefaultConfigProps() {
    final Properties props = new Properties();
    props.setProperty(PROP_PREFIX + "gracefulShutdownTimeout", "15000");

    props.setProperty(STRATEGY_PROPERTY_NAME, UBER.name());
    props.setProperty(UBER_THREAD_POOL_SIZE_CORE, "cores");
    props.setProperty(UBER_THREAD_POOL_SIZE_MAX, "max(2, cores + ((mem - 245760) / 5120))");
    props.setProperty(UBER_QUEUE_SIZE, "mem / (2*3*32)");
    props.setProperty(UBER_THREAD_POOL_KEEP_ALIVE, "30000");

    return props;
  }

  @Test
  public void noMuleHome() throws IOException, MuleException {
    clearProperty(MULE_HOME_DIRECTORY_PROPERTY);

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertDefaultSettings(config);
  }

  private void assertDefaultSettings(SchedulerPoolsConfig config) {
    assertThat(config.getGracefulShutdownTimeout().getAsLong(), is(15000l));
    assertThat(config.getSchedulerPoolStrategy(), is(UBER));
    assertThat(config.getCpuLightPoolSize().isPresent(), is(false));
    assertThat(config.getCpuLightQueueSize().isPresent(), is(false));
    assertThat(config.getIoCorePoolSize().isPresent(), is(false));
    assertThat(config.getIoMaxPoolSize().isPresent(), is(false));
    assertThat(config.getIoKeepAlive().isPresent(), is(false));

    assertThat(config.getUberCorePoolSize().getAsInt(), is(CORES));
    assertThat(config.getUberMaxPoolSize().getAsInt(), is(BIG_POOL_DEFAULT_SIZE));


    assertThat(config.getUberQueueSize().isPresent(), is(true));
    int queueSize = config.getUberQueueSize().getAsInt();
    if (queueSize > 0) {
      assertThat(queueSize, is((int) MEM / (2 * 3 * 32)));
    } else {
      assertThat(queueSize, is(0));
    }

    assertThat(config.getUberKeepAlive().getAsLong(), is(30000l));

    assertThat(config.getCpuIntensivePoolSize().isPresent(), is(false));
    assertThat(config.getCpuIntensiveQueueSize().isPresent(), is(false));
  }

  @Test
  public void noConfigFile() throws MuleException {
    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertDefaultSettings(config);
  }

  @Test
  public void defaultConfig() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.store(new FileOutputStream(schedulerConfigFile), "defaultConfig");

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertDefaultSettings(config);
  }

  @Test
  public void dedicatedConfigSpaced() throws IOException, MuleException {
    final Properties props = new Properties();

    props.setProperty(STRATEGY_PROPERTY_NAME, DEDICATED.name());
    props.setProperty(CPU_LIGHT_PREFIX + "." + THREAD_POOL_SIZE_CORE, "2 *cores");
    props.setProperty(CPU_LIGHT_PREFIX + "." + WORK_QUEUE_SIZE, "mem/ (2* 3*32 )");
    props.setProperty(IO_PREFIX + "." + THREAD_POOL_SIZE_MAX, "cores* cores");
    props.setProperty(IO_PREFIX + "." + WORK_QUEUE_SIZE, "mem /( 2*3*32)");
    props.setProperty(CPU_INTENSIVE_PREFIX + "." + THREAD_POOL_SIZE_CORE, "2  *   cores");
    props.setProperty(CPU_INTENSIVE_PREFIX + "." + WORK_QUEUE_SIZE, "mem / ( 2*3*32) ");
    props.store(new FileOutputStream(schedulerConfigFile), "defaultConfigSpaced");

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertThat(config.getSchedulerPoolStrategy(), is(DEDICATED));
    assertThat(config.getGracefulShutdownTimeout().getAsLong(), is(15000l));
    assertThat(config.getCpuLightPoolSize().getAsInt(), is(2 * CORES));
    assertThat(config.getCpuLightQueueSize().getAsInt(), is((int) (MEM / (2 * 3 * 32))));
    assertThat(config.getIoCorePoolSize().getAsInt(), is(CORES));
    assertThat(config.getIoMaxPoolSize().getAsInt(), is(CORES * CORES));
    assertThat(config.getIoQueueSize().getAsInt(), is((int) (MEM / (2 * 3 * 32))));
    assertThat(config.getIoKeepAlive().getAsLong(), is(30000l));
    assertThat(config.getCpuIntensivePoolSize().getAsInt(), is(2 * CORES));
    assertThat(config.getCpuIntensiveQueueSize().getAsInt(), is((int) (MEM / (2 * 3 * 32))));
  }

  @Test
  public void withDecimalsConfig() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(UBER_THREAD_POOL_SIZE_CORE, "0.5 *cores");
    props.setProperty(UBER_THREAD_POOL_SIZE_MAX, "mem / (2* 2.5 *32)");
    props.store(new FileOutputStream(schedulerConfigFile), "withDecimalsConfig");

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertThat(config.getUberCorePoolSize().getAsInt(), is(CORES / 2));
    assertThat(config.getUberMaxPoolSize().getAsInt(), is((int) (MEM / (2 * 2.5 * 32))));
  }

  @Test
  public void withPlusAndMinusConfig() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(UBER_THREAD_POOL_SIZE_CORE, "cores - 1");
    props.setProperty(UBER_THREAD_POOL_SIZE_MAX, "cores + cores");
    props.store(new FileOutputStream(schedulerConfigFile), "withPlusAndMinusConfig");

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertThat(config.getUberCorePoolSize().getAsInt(), is(CORES - 1));
    assertThat(config.getUberMaxPoolSize().getAsInt(), is(CORES + CORES));
  }

  @Test
  public void withMultiplyAndDivisionConfig() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(UBER_THREAD_POOL_SIZE_CORE, "cores / 0.5");
    props.setProperty(UBER_THREAD_POOL_SIZE_MAX, "cores * 2");
    props.store(new FileOutputStream(schedulerConfigFile), "withMultiplyAndDivisionConfig");

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertThat(config.getUberCorePoolSize().getAsInt(), is(2 * CORES));
    assertThat(config.getUberMaxPoolSize().getAsInt(), is(2 * CORES));
  }

  @Test
  public void withParenthesisConfig() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(UBER_THREAD_POOL_SIZE_CORE, "cores * (1+1)");
    props.setProperty(UBER_THREAD_POOL_SIZE_MAX, "(cores + 1) * 2");
    props.store(new FileOutputStream(schedulerConfigFile), "withParenthesisConfig");

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertThat(config.getUberCorePoolSize().getAsInt(), is(2 * CORES));
    assertThat(config.getUberMaxPoolSize().getAsInt(), is(2 * (1 + CORES)));
  }

  @Test
  public void expressionConfigFixed() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(UBER_THREAD_POOL_SIZE_CORE, "2");
    props.setProperty(UBER_THREAD_POOL_SIZE_MAX, "8");
    props.setProperty(UBER_QUEUE_SIZE, "4");
    props.store(new FileOutputStream(schedulerConfigFile), "expressionConfigFixed");

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertThat(config.getUberCorePoolSize().getAsInt(), is(2));
    assertThat(config.getUberMaxPoolSize().getAsInt(), is(8));
    assertThat(config.getUberQueueSize().getAsInt(), is(4));
  }

  @Test
  public void expressionConfigNegative() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(STRATEGY_PROPERTY_NAME, DEDICATED.name());
    props.setProperty(CPU_LIGHT_PREFIX + "." + THREAD_POOL_SIZE, "cores - " + (CORES + 1));
    props.store(new FileOutputStream(schedulerConfigFile), "expressionConfigNegative");

    expected.expect(DefaultMuleException.class);
    expected.expectMessage(is(CPU_LIGHT_PREFIX + "." + THREAD_POOL_SIZE + ": Value has to be greater than 0"));
    loadThreadPoolsConfig();
  }

  @Test
  public void zeroWorkQueueSize() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(UBER_QUEUE_SIZE, "0");
    props.store(new FileOutputStream(schedulerConfigFile), "expressionConfigNegative");

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertThat(config.getUberQueueSize().getAsInt(), is(0));
  }

  @Test
  public void invalidExpressionConfig() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(STRATEGY_PROPERTY_NAME, DEDICATED.name());
    props.setProperty(CPU_LIGHT_PREFIX + "." + THREAD_POOL_SIZE, "invalid");
    props.store(new FileOutputStream(schedulerConfigFile), "invalidExpressionConfig");

    expected.expect(DefaultMuleException.class);
    expected.expectMessage(is(CPU_LIGHT_PREFIX + "." + THREAD_POOL_SIZE + ": Expression not valid"));
    loadThreadPoolsConfig();
  }

  @Test
  public void nastyExpressionConfig() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(STRATEGY_PROPERTY_NAME, DEDICATED.name());
    props.setProperty(CPU_LIGHT_PREFIX + "." + THREAD_POOL_SIZE, "; print('aha!')");
    props.store(new FileOutputStream(schedulerConfigFile), "nastyExpressionConfig");

    expected.expect(DefaultMuleException.class);
    expected.expectMessage(is(CPU_LIGHT_PREFIX + "." + THREAD_POOL_SIZE + ": Expression not valid"));
    loadThreadPoolsConfig();
  }

  @Test
  public void invalidShutdownTimeConfig() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(PROP_PREFIX + "gracefulShutdownTimeout", "cores");
    props.store(new FileOutputStream(schedulerConfigFile), "invalidShutdownTimeConfig");

    expected.expect(DefaultMuleException.class);
    expected.expectCause(instanceOf(NumberFormatException.class));
    expected.expectMessage(is(PROP_PREFIX + "gracefulShutdownTimeout: For input string: \"cores\""));
    loadThreadPoolsConfig();
  }

  @Test
  public void invalidIoKeepAliveConfig() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(STRATEGY_PROPERTY_NAME, DEDICATED.name());
    props.setProperty(IO_PREFIX + "." + THREAD_POOL_KEEP_ALIVE, "notANumber");
    props.store(new FileOutputStream(schedulerConfigFile), "invalidIoKeepAliveConfig");

    expected.expect(DefaultMuleException.class);
    expected.expectCause(instanceOf(NumberFormatException.class));
    expected.expectMessage(is(IO_PREFIX + "." + THREAD_POOL_KEEP_ALIVE + ": For input string: \"notANumber\""));
    loadThreadPoolsConfig();
  }

  @Test
  public void negativeShutdownTimeConfig() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();

    props.setProperty(STRATEGY_PROPERTY_NAME, UBER.name());
    props.setProperty(PROP_PREFIX + "gracefulShutdownTimeout", "-1");
    props.store(new FileOutputStream(schedulerConfigFile), "negativeShutdownTimeConfig");

    expected.expect(DefaultMuleException.class);
    expected.expectMessage(is(PROP_PREFIX + "gracefulShutdownTimeout: Value has to be greater than or equal to 0"));
    loadThreadPoolsConfig();
  }

  @Test
  public void negativeIoKeepAliveConfig() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(STRATEGY_PROPERTY_NAME, DEDICATED.name());
    props.setProperty(IO_PREFIX + "." + THREAD_POOL_KEEP_ALIVE, "-2");
    props.store(new FileOutputStream(schedulerConfigFile), "negativeIoKeepAliveConfig");

    expected.expect(DefaultMuleException.class);
    expected.expectMessage(is(IO_PREFIX + "." + THREAD_POOL_KEEP_ALIVE + ": Value has to be greater than or equal to 0"));
    loadThreadPoolsConfig();
  }

  @Test
  public void unevenParenthesis() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.setProperty(STRATEGY_PROPERTY_NAME, DEDICATED.name());
    props.setProperty(IO_PREFIX + "." + WORK_QUEUE_SIZE, "(-2");
    props.store(new FileOutputStream(schedulerConfigFile), "unevenParenthesis");

    expected.expect(DefaultMuleException.class);
    expected.expectMessage(startsWith(IO_PREFIX + "." + WORK_QUEUE_SIZE));
    expected.expectMessage(containsString("<eval>:1:3 Expected ) but found eof"));
    loadThreadPoolsConfig();
  }

  @Test
  @Description("For a missing entry in the config file, the default value is used")
  public void missingExpression() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.remove(UBER_THREAD_POOL_SIZE_MAX);
    props.store(new FileOutputStream(schedulerConfigFile), "defaultConfig");

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertThat(config.getUberMaxPoolSize().getAsInt(), is(BIG_POOL_DEFAULT_SIZE));
  }

  @Test
  @Description("For a missing entry in the config file, the default value is used")
  public void missingValue() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.remove(UBER_THREAD_POOL_KEEP_ALIVE);
    props.store(new FileOutputStream(schedulerConfigFile), "defaultConfig");

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertThat(config.getUberKeepAlive().getAsLong(), is(30000l));
  }

  @Test
  @Description("Tests that the mule.schedulerPools.configFile property is honored if present")
  public void overrideConfigFile() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.put(UBER_THREAD_POOL_SIZE_MAX, "100");

    File overrideConfigFile = new File(tempOtherDir.getRoot(), "overriding.conf");
    props.store(new FileOutputStream(overrideConfigFile), "defaultConfig");
    System.setProperty(SCHEDULER_POOLS_CONFIG_FILE_PROPERTY, overrideConfigFile.getPath());

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertThat(config.getUberMaxPoolSize().getAsInt(), is(100));
  }

  @Test
  @Description("Tests that the mule.schedulerPools.configFile pointing to an external url property is honored if present")
  @Ignore("Uncomment when we actually have a url with the new parameters")
  public void overrideConfigFileWithUrl() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.put(UBER_THREAD_POOL_SIZE_MAX, "1");

    System.setProperty(SCHEDULER_POOLS_CONFIG_FILE_PROPERTY,
                       "https://raw.githubusercontent.com/mulesoft/mule-distributions/mule-4.2.0/standalone/src/main/resources/conf/scheduler-pools.conf");

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertThat(config.getIoCorePoolSize().getAsInt(), is(CORES));
  }

  @Test
  @Description("Tests that system properties overriding the config from the file are honored if present")
  @Ignore("Uncomment when we actually have a url with the new paramters")
  public void overrideConfigWithIndividualProperty() throws IOException, MuleException {
    final Properties props = buildDefaultConfigProps();
    props.put(UBER_THREAD_POOL_SIZE_MAX, "1");

    System.setProperty(SCHEDULER_POOLS_CONFIG_FILE_PROPERTY,
                       "https://raw.githubusercontent.com/mulesoft/mule-distributions/mule-4.2.0/standalone/src/main/resources/conf/scheduler-pools.conf");
    System.setProperty("org.mule.runtime.scheduler.io.threadPool.maxSize",
                       "100");

    final SchedulerPoolsConfig config = loadThreadPoolsConfig();

    assertThat(config.getUberMaxPoolSize().getAsInt(), is(100));
  }

}
