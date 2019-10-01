/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal.config;

import static java.io.File.separator;
import static java.lang.Long.parseLong;
import static java.lang.Math.max;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.util.regex.Pattern.compile;
import static org.mule.runtime.api.i18n.I18nMessageFactory.createStaticMessage;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.DEDICATED;
import static org.mule.runtime.api.scheduler.SchedulerPoolStrategy.UBER;
import static org.mule.runtime.core.api.config.MuleProperties.MULE_HOME_DIRECTORY_PROPERTY;
import static org.mule.service.scheduler.ThreadType.CPU_INTENSIVE;
import static org.mule.service.scheduler.ThreadType.CPU_LIGHT;
import static org.mule.service.scheduler.ThreadType.IO;
import static org.slf4j.LoggerFactory.getLogger;

import org.mule.runtime.api.exception.DefaultMuleException;
import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.scheduler.SchedulerPoolStrategy;
import org.mule.runtime.api.scheduler.SchedulerPoolsConfig;
import org.mule.runtime.core.api.config.ConfigurationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.slf4j.Logger;

/**
 * Bean that contains the thread pools configuration for the runtime.
 * <p>
 * All of its getter methods for configuration values return non-null values.
 *
 * @since 1.0
 */
public class ContainerThreadPoolsConfig implements SchedulerPoolsConfig {

  private static final Logger LOGGER = getLogger(ContainerThreadPoolsConfig.class);

  public static final String PROP_PREFIX = "org.mule.runtime.scheduler.";

  private static final Pattern MAX_MIN_PATTERN = compile("(max|min)");
  private static final String NUMBER_OR_VAR_REGEXP = "([0-9]+(\\.[0-9]+)?)|cores|mem";
  private static final String FORMULA_FUNCTION_PARAM =
      "(" + NUMBER_OR_VAR_REGEXP + ")?(\\s*[-+\\/*\\(\\)]\\s*(" + NUMBER_OR_VAR_REGEXP + ")?)*";
  private static final Pattern POOLSIZE_PATTERN =
      compile("^" + FORMULA_FUNCTION_PARAM
          + "|max\\s*\\(\\s*(" + FORMULA_FUNCTION_PARAM + ")?\\s*,\\s*(" + FORMULA_FUNCTION_PARAM + ")?\\s*\\)"
          + "|min\\s*\\(\\s*(" + FORMULA_FUNCTION_PARAM + ")?\\s*,\\s*(" + FORMULA_FUNCTION_PARAM + ")?\\s*\\)$");



  public static final String SCHEDULER_POOLS_CONFIG_FILE_PROPERTY = "mule.schedulerPools.configFile";
  public static final String CONF_FILE_NAME = "conf" + separator + "scheduler-pools.conf";
  public static final String STRATEGY_PROPERTY_NAME = PROP_PREFIX + SchedulerPoolStrategy.class.getSimpleName();
  public static final String CPU_LIGHT_PREFIX = PROP_PREFIX + CPU_LIGHT.getName();
  public static final String IO_PREFIX = PROP_PREFIX + IO.getName();
  public static final String CPU_INTENSIVE_PREFIX = PROP_PREFIX + CPU_INTENSIVE.getName();
  public static final String UBER_PREFIX = PROP_PREFIX + "uber";
  public static final String THREAD_POOL = "threadPool";
  public static final String THREAD_POOL_SIZE = THREAD_POOL + ".size";
  public static final String CPU_INTENSIVE_THREAD_POOL_SIZE = CPU_INTENSIVE_PREFIX + "." + THREAD_POOL_SIZE;
  public static final String CPU_LIGHT_THREAD_POOL_SIZE = CPU_LIGHT_PREFIX + "." + THREAD_POOL_SIZE;
  public static final String THREAD_POOL_SIZE_MAX = THREAD_POOL + ".maxSize";
  public static final String UBER_THREAD_POOL_SIZE_MAX = UBER_PREFIX + "." + THREAD_POOL_SIZE_MAX;
  public static final String IO_THREAD_POOL_SIZE_MAX = IO_PREFIX + "." + THREAD_POOL_SIZE_MAX;
  public static final String THREAD_POOL_SIZE_CORE = THREAD_POOL + ".coreSize";
  public static final String UBER_THREAD_POOL_SIZE_CORE = UBER_PREFIX + "." + THREAD_POOL_SIZE_CORE;
  public static final String IO_THREAD_POOL_SIZE = IO_PREFIX + "." + THREAD_POOL_SIZE_CORE;
  public static final String THREAD_POOL_KEEP_ALIVE = THREAD_POOL + ".threadKeepAlive";
  public static final String UBER_THREAD_POOL_KEEP_ALIVE = UBER_PREFIX + "." + THREAD_POOL_KEEP_ALIVE;
  public static final String IO_THERAD_POOL_KEEP_ALIVE = IO_PREFIX + "." + THREAD_POOL_KEEP_ALIVE;
  public static final String WORK_QUEUE = "workQueue";
  public static final String WORK_QUEUE_SIZE = WORK_QUEUE + ".size";
  public static final String UBER_QUEUE_SIZE = UBER_PREFIX + "." + WORK_QUEUE_SIZE;
  public static final String CPU_INTENSIVE_WORK_QUEUE_SIZE = CPU_INTENSIVE_PREFIX + "." + WORK_QUEUE_SIZE;
  public static final String IO_WORK_QUEUE_SIZE = IO_PREFIX + "." + WORK_QUEUE_SIZE;
  public static final String CPU_LIGHT_WORK_QUEUE_SIZE = CPU_LIGHT_PREFIX + "." + WORK_QUEUE_SIZE;

  /**
   * Loads the configuration from the {@code &#123;mule.home&#125;/conf/scheduler-pools.conf} file.
   *
   * @return The loaded configuration, or the default if the file is unavailable.
   * @throws MuleException for any trouble that happens while parsing the file.
   */
  public static ContainerThreadPoolsConfig loadThreadPoolsConfig() throws MuleException {
    final ContainerThreadPoolsConfig config = new ContainerThreadPoolsConfig();

    String overriddenConfigFileName = getProperty(SCHEDULER_POOLS_CONFIG_FILE_PROPERTY);

    if (overriddenConfigFileName != null) {
      File overriddenConfigFile = new File(overriddenConfigFileName);

      if (overriddenConfigFile.exists()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Loading thread pools configuration from " + overriddenConfigFile.getPath());
        }

        try (final FileInputStream configIs = new FileInputStream(overriddenConfigFile)) {
          return loadProperties(config, configIs);
        } catch (IOException e) {
          throw new DefaultMuleException(e);
        }
      } else {
        try (final InputStream configIs = new URL(overriddenConfigFileName).openStream();) {
          return loadProperties(config, configIs);
        } catch (IOException e) {
          throw new DefaultMuleException(e);
        }
      }
    }

    File muleHome =
        getProperty(MULE_HOME_DIRECTORY_PROPERTY) != null ? new File(getProperty(MULE_HOME_DIRECTORY_PROPERTY)) : null;

    if (muleHome == null) {
      LOGGER.info("No " + MULE_HOME_DIRECTORY_PROPERTY + " defined. Using default values for thread pools.");
      return config;
    }

    File defaultConfigFile = new File(muleHome, CONF_FILE_NAME);
    if (!defaultConfigFile.exists()) {
      LOGGER.info("No thread pools config file found. Using default values.");
      return config;
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Loading thread pools configuration from " + defaultConfigFile.getPath());
    }

    try (final FileInputStream configIs = new FileInputStream(defaultConfigFile)) {
      return loadProperties(config, configIs);
    } catch (IOException e) {
      throw new DefaultMuleException(e);
    }
  }

  private static ContainerThreadPoolsConfig loadProperties(final ContainerThreadPoolsConfig config, InputStream configIs)
      throws MuleException {
    final Properties properties = new Properties();
    try {
      properties.load(configIs);
    } catch (IOException e) {
      throw new DefaultMuleException(e);
    }

    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("js");
    if (engine == null) {
      throw new ConfigurationException(
                                       createStaticMessage("No 'js' script engine found. It is required to parse the config in 'conf/scheduler-pools.conf'"));
    }
    engine.put("cores", CORES);
    engine.put("mem", MEM);

    config.setSchedulerPoolStrategy(resolveSchedulerPoolStrategy(properties), true);

    resolveNumber(properties, PROP_PREFIX + "gracefulShutdownTimeout", true)
        .ifPresent(v -> config.setGracefulShutdownTimeout(v));

    resolveExpression(properties, UBER_THREAD_POOL_SIZE_CORE, engine, false)
        .ifPresent(v -> config.setUberCorePoolSize(v));
    resolveExpression(properties, UBER_THREAD_POOL_SIZE_MAX, engine, false)
        .ifPresent(v -> config.setUberMaxPoolSize(v));
    resolveExpression(properties, UBER_QUEUE_SIZE, engine, true)
        .ifPresent(v -> config.setUberQueueSize(v));
    resolveNumber(properties, UBER_THREAD_POOL_KEEP_ALIVE, true)
        .ifPresent(v -> config.setUberKeepAlive(v));

    resolveExpression(properties, CPU_LIGHT_THREAD_POOL_SIZE, engine, false)
        .ifPresent(v -> config.setCpuLightPoolSize(v));
    resolveExpression(properties, CPU_LIGHT_WORK_QUEUE_SIZE, engine, true)
        .ifPresent(v -> config.setCpuLightQueueSize(v));

    resolveExpression(properties, IO_THREAD_POOL_SIZE, engine, false)
        .ifPresent(v -> config.setIoCorePoolSize(v));
    resolveExpression(properties, IO_THREAD_POOL_SIZE_MAX, engine, false)
        .ifPresent(v -> config.setIoMaxPoolSize(v));
    resolveExpression(properties, IO_WORK_QUEUE_SIZE, engine, true)
        .ifPresent(v -> config.setIoQueueSize(v));
    resolveNumber(properties, IO_THERAD_POOL_KEEP_ALIVE, true)
        .ifPresent(v -> config.setIoKeepAlive(v));

    resolveExpression(properties, CPU_INTENSIVE_THREAD_POOL_SIZE, engine, false)
        .ifPresent(v -> config.setCpuIntensivePoolSize(v));
    resolveExpression(properties, CPU_INTENSIVE_WORK_QUEUE_SIZE, engine, true)
        .ifPresent(v -> config.setCpuIntensiveQueueSize(v));

    return config;
  }

  private static SchedulerPoolStrategy resolveSchedulerPoolStrategy(Properties properties) throws ConfigurationException {
    String strategyName = resolvePropertyValue(properties, STRATEGY_PROPERTY_NAME);
    if (strategyName == null) {
      throw new ConfigurationException(
                                       createStaticMessage(format("Property '%s' was not specified in file '%s'",
                                                                  STRATEGY_PROPERTY_NAME, CONF_FILE_NAME)));
    }

    try {
      return SchedulerPoolStrategy.valueOf(strategyName.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException(createStaticMessage(
                                                           format("There's no %s named '%s'",
                                                                  SchedulerPoolStrategy.class.getSimpleName(), strategyName)));
    }
  }

  private static OptionalLong resolveNumber(Properties properties, String propName, boolean allowZero) throws MuleException {
    if (!properties.containsKey(propName)) {
      LOGGER.warn("No property '{}' found in config file. Using default value.", propName);
      return OptionalLong.empty();
    }

    final String property = properties.getProperty(propName);
    try {
      final long result = parseLong(property);
      validateNumber(propName, result, allowZero);

      return OptionalLong.of(result);
    } catch (NumberFormatException e) {
      throw new DefaultMuleException(propName + ": " + e.getMessage(), e);
    }
  }

  private static OptionalInt resolveExpression(Properties properties,
                                               String propName,
                                               ScriptEngine engine,
                                               boolean allowZero)
      throws MuleException {

    String property = resolvePropertyValue(properties, propName);

    if (property == null) {
      LOGGER.warn("No property '{}' found in config file. Using default value.", propName);
      return OptionalInt.empty();
    }

    if (!POOLSIZE_PATTERN.matcher(property).matches()) {
      throw new DefaultMuleException(propName + ": Expression not valid");
    }

    property = MAX_MIN_PATTERN.matcher(property).replaceAll("Math.$1");

    try {
      final int result = ((Number) engine.eval(property)).intValue();
      validateNumber(propName, result, allowZero);

      return OptionalInt.of(result);
    } catch (ScriptException e) {
      throw new DefaultMuleException(propName + ": " + e.getMessage(), e);
    }
  }

  private static String resolvePropertyValue(Properties properties, String propName) {
    String property = null;

    final String sysPropOverride = getProperty(propName);
    if (sysPropOverride != null) {
      LOGGER.debug("Found system property override for '{}' with value '{}'", propName, sysPropOverride);

      property = sysPropOverride.trim().toLowerCase();
    } else if (properties.containsKey(propName)) {
      property = properties.getProperty(propName).trim().toLowerCase();
    }
    return property;
  }

  private static void validateNumber(String propName, long result, boolean allowZero) throws MuleException {
    if (allowZero) {
      if (result < 0) {
        throw new DefaultMuleException(propName + ": Value has to be greater than or equal to 0");
      }

    } else {
      if (result <= 0) {
        throw new DefaultMuleException(propName + ": Value has to be greater than 0");
      }
    }
  }

  private static int CORES = getRuntime().availableProcessors();
  private static long MEM = getRuntime().maxMemory() / 1024;

  private SchedulerPoolStrategy schedulerPoolStrategy = UBER;
  private long gracefulShutdownTimeout = 15000;
  private Integer cpuLightQueueSize = null;
  private Integer cpuLightPoolSize = null;
  private Integer ioQueueSize = null;
  private Integer ioCorePoolSize = null;
  private Integer ioMaxPoolSize = null;
  private Long ioKeepAlive = null;
  private Integer uberQueueSize = 0;
  private Integer uberCorePoolSize = CORES;
  private Integer uberMaxPoolSize = (int) max(2, CORES + ((MEM - 245760) / 5120));
  private Long uberKeepAlive = 30000L;
  private Integer cpuIntensiveQueueSize = null;
  private Integer cpuIntensivePoolSize = null;

  private ContainerThreadPoolsConfig() {}

  @Override
  public OptionalLong getGracefulShutdownTimeout() {
    return OptionalLong.of(gracefulShutdownTimeout);
  }

  private void setGracefulShutdownTimeout(long gracefulShutdownTimeout) {
    this.gracefulShutdownTimeout = gracefulShutdownTimeout;
  }

  @Override
  public SchedulerPoolStrategy getSchedulerPoolStrategy() {
    return schedulerPoolStrategy;
  }

  public void setSchedulerPoolStrategy(SchedulerPoolStrategy schedulerPoolStrategy) {
    setSchedulerPoolStrategy(schedulerPoolStrategy, false);
  }

  public void setSchedulerPoolStrategy(SchedulerPoolStrategy schedulerPoolStrategy, boolean initDefaults) {
    this.schedulerPoolStrategy = schedulerPoolStrategy;
    if (initDefaults) {
      if (schedulerPoolStrategy == DEDICATED) {
        cpuLightQueueSize = 0;
        cpuLightPoolSize = 2 * CORES;
        ioQueueSize = 0;
        ioCorePoolSize = CORES;
        ioMaxPoolSize = (int) max(2, CORES + ((MEM - 245760) / 5120));
        ioKeepAlive = 30000L;
        cpuIntensiveQueueSize = 2 * CORES;
        cpuIntensivePoolSize = 2 * CORES;

        uberCorePoolSize = uberMaxPoolSize = uberQueueSize = null;
        uberKeepAlive = null;
      } else {
        uberQueueSize = 0;
        uberCorePoolSize = CORES;
        uberMaxPoolSize = (int) max(2, CORES + ((MEM - 245760) / 5120));
        uberKeepAlive = 30000L;

        cpuLightQueueSize = cpuLightPoolSize = null;
        ioQueueSize = ioCorePoolSize = ioMaxPoolSize = null;
        ioKeepAlive = null;
        cpuIntensiveQueueSize = cpuIntensivePoolSize = null;
      }
    }
  }

  @Override
  public OptionalInt getUberCorePoolSize() {
    return ofNullable(uberCorePoolSize);
  }

  public void setUberCorePoolSize(Integer uberCorePoolSize) {
    assertUberStrategy(UBER_THREAD_POOL_SIZE_CORE);
    this.uberCorePoolSize = uberCorePoolSize;
  }

  @Override
  public OptionalInt getUberMaxPoolSize() {
    return nullableMax(uberCorePoolSize, uberMaxPoolSize);
  }

  public void setUberMaxPoolSize(Integer uberMaxPoolSize) {
    assertUberStrategy(UBER_THREAD_POOL_SIZE_MAX);
    this.uberMaxPoolSize = uberMaxPoolSize;
  }

  @Override
  public OptionalInt getUberQueueSize() {
    return ofNullable(uberQueueSize);
  }

  public void setUberQueueSize(Integer uberQueueSize) {
    assertUberStrategy(UBER_QUEUE_SIZE);
    this.uberQueueSize = uberQueueSize;
  }

  @Override
  public OptionalLong getUberKeepAlive() {
    return ofNullable(uberKeepAlive);
  }

  public void setUberKeepAlive(Long uberKeepAlive) {
    assertUberStrategy(UBER_THREAD_POOL_KEEP_ALIVE);
    this.uberKeepAlive = uberKeepAlive;
  }

  @Override
  public OptionalInt getCpuLightPoolSize() {
    return ofNullable(cpuLightPoolSize);
  }

  private void setCpuLightPoolSize(int cpuLightPoolSize) {
    assertDedicatedStrategy(CPU_LIGHT_THREAD_POOL_SIZE);
    this.cpuLightPoolSize = cpuLightPoolSize;
  }

  @Override
  public OptionalInt getCpuLightQueueSize() {
    return ofNullable(cpuLightQueueSize);
  }

  private void setCpuLightQueueSize(int cpuLightQueueSize) {
    assertDedicatedStrategy(CPU_LIGHT_WORK_QUEUE_SIZE);
    this.cpuLightQueueSize = cpuLightQueueSize;
  }

  @Override
  public OptionalInt getIoCorePoolSize() {
    return ofNullable(ioCorePoolSize);
  }

  private void setIoCorePoolSize(int ioCorePoolSize) {
    assertDedicatedStrategy(IO_THREAD_POOL_SIZE);
    this.ioCorePoolSize = ioCorePoolSize;
  }

  @Override
  public OptionalInt getIoMaxPoolSize() {
    return nullableMax(ioCorePoolSize, ioMaxPoolSize);
  }

  private void setIoMaxPoolSize(int ioMaxPoolSize) {
    assertDedicatedStrategy(IO_THREAD_POOL_SIZE_MAX);
    this.ioMaxPoolSize = ioMaxPoolSize;
  }

  @Override
  public OptionalInt getIoQueueSize() {
    return ofNullable(ioQueueSize);
  }

  private void setIoQueueSize(int ioQueueSize) {
    assertDedicatedStrategy(IO_WORK_QUEUE_SIZE);
    this.ioQueueSize = ioQueueSize;
  }

  @Override
  public OptionalLong getIoKeepAlive() {
    return ofNullable(ioKeepAlive);
  }

  private void setIoKeepAlive(long ioKeepAlive) {
    assertDedicatedStrategy(IO_THERAD_POOL_KEEP_ALIVE);
    this.ioKeepAlive = ioKeepAlive;
  }

  @Override
  public OptionalInt getCpuIntensivePoolSize() {
    return ofNullable(cpuIntensivePoolSize);
  }

  private void setCpuIntensivePoolSize(int cpuIntensivePoolSize) {
    assertDedicatedStrategy(CPU_INTENSIVE_THREAD_POOL_SIZE);
    this.cpuIntensivePoolSize = cpuIntensivePoolSize;
  }

  @Override
  public OptionalInt getCpuIntensiveQueueSize() {
    return ofNullable(cpuIntensiveQueueSize);
  }

  private void setCpuIntensiveQueueSize(int cpuIntensiveQueueSize) {
    assertDedicatedStrategy(CPU_INTENSIVE_WORK_QUEUE_SIZE);
    this.cpuIntensiveQueueSize = cpuIntensiveQueueSize;
  }

  @Override
  public String getThreadNamePrefix() {
    return "[MuleRuntime].";
  }

  private OptionalInt nullableMax(Integer a, Integer b) {
    if (a == null && b == null) {
      return OptionalInt.empty();
    } else if (a == null && b != null) {
      return OptionalInt.of(b);
    } else if (a != null && b == null) {
      return OptionalInt.of(a);
    } else {
      return OptionalInt.of(max(a, b));
    }
  }

  private OptionalInt ofNullable(Integer value) {
    return value != null ? OptionalInt.of(value) : OptionalInt.empty();
  }

  private OptionalLong ofNullable(Long value) {
    return value != null ? OptionalLong.of(value) : OptionalLong.empty();
  }

  private void assertDedicatedStrategy(String propertyName) {
    assertStrategy(DEDICATED, propertyName);
  }

  private void assertUberStrategy(String propertyName) {
    assertStrategy(UBER, propertyName);
  }

  private void assertStrategy(SchedulerPoolStrategy expected, String propertyName) {
    if (schedulerPoolStrategy != expected) {
      throw new IllegalStateException(new ConfigurationException(createStaticMessage(format(
                                                                                            "Property '%s' can only be set when '%s' is '%s'. Current value is '%s'",
                                                                                            propertyName,
                                                                                            STRATEGY_PROPERTY_NAME,
                                                                                            expected,
                                                                                            schedulerPoolStrategy))));
    }
  }
}
