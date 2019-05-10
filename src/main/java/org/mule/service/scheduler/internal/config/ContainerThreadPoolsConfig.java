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
import static java.lang.System.getProperty;
import static java.util.regex.Pattern.compile;
import static org.mule.runtime.api.i18n.I18nMessageFactory.createStaticMessage;
import static org.mule.runtime.core.api.config.MuleProperties.MULE_HOME_DIRECTORY_PROPERTY;
import static org.mule.service.scheduler.ThreadType.CPU_INTENSIVE;
import static org.mule.service.scheduler.ThreadType.CPU_LIGHT;
import static org.mule.service.scheduler.ThreadType.IO;
import static org.slf4j.LoggerFactory.getLogger;

import org.mule.runtime.api.exception.DefaultMuleException;
import org.mule.runtime.api.exception.MuleException;
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
  private static final Pattern MAX_MIN_PATTERN = compile("(max|min)");
  public static final String SCHEDULER_POOLS_CONFIG_FILE_PROPERTY = "mule.schedulerPools.configFile";

  public static final String PROP_PREFIX = "org.mule.runtime.scheduler.";
  public static final String CPU_LIGHT_PREFIX = PROP_PREFIX + CPU_LIGHT.getName();
  public static final String IO_PREFIX = PROP_PREFIX + IO.getName();
  public static final String CPU_INTENSIVE_PREFIX = PROP_PREFIX + CPU_INTENSIVE.getName();
  public static final String THREAD_POOL = "threadPool";
  public static final String THREAD_POOL_SIZE = THREAD_POOL + ".size";
  public static final String THREAD_POOL_SIZE_MAX = THREAD_POOL + ".maxSize";
  public static final String THREAD_POOL_SIZE_CORE = THREAD_POOL + ".coreSize";
  public static final String THREAD_POOL_KEEP_ALIVE = THREAD_POOL + ".threadKeepAlive";
  public static final String WORK_QUEUE = "workQueue";
  public static final String WORK_QUEUE_SIZE = WORK_QUEUE + ".size";

  private static final String NUMBER_OR_VAR_REGEXP = "([0-9]+(\\.[0-9]+)?)|cores|mem";
  private static final String FORMULA_FUNCTION_PARAM =
      "(" + NUMBER_OR_VAR_REGEXP + ")?(\\s*[-+\\/*\\(\\)]\\s*(" + NUMBER_OR_VAR_REGEXP + ")?)*";
  private static final Pattern POOLSIZE_PATTERN =
      compile("^" + FORMULA_FUNCTION_PARAM
          + "|max\\s*\\(\\s*(" + FORMULA_FUNCTION_PARAM + ")?\\s*,\\s*(" + FORMULA_FUNCTION_PARAM + ")?\\s*\\)"
          + "|min\\s*\\(\\s*(" + FORMULA_FUNCTION_PARAM + ")?\\s*,\\s*(" + FORMULA_FUNCTION_PARAM + ")?\\s*\\)$");

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

    File defaultConfigFile = new File(muleHome, "conf" + separator + "scheduler-pools.conf");
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
      throws DefaultMuleException, ConfigurationException, MuleException {
    final Properties properties = new Properties();
    try {
      properties.load(configIs);
    } catch (IOException e) {
      throw new DefaultMuleException(e);
    }

    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("js");
    if (engine == null) {
      throw new ConfigurationException(createStaticMessage("No 'js' script engine found. It is required to parse the config in 'conf/scheduler-pools.conf'"));
    }
    engine.put("cores", cores);
    engine.put("mem", mem);

    resolveNumber(properties, PROP_PREFIX + "gracefulShutdownTimeout", true)
        .ifPresent(v -> config.setGracefulShutdownTimeout(v));

    resolveExpression(properties, CPU_LIGHT_PREFIX + "." + THREAD_POOL_SIZE, config, engine, false)
        .ifPresent(v -> config.setCpuLightPoolSize(v));
    resolveExpression(properties, CPU_LIGHT_PREFIX + "." + WORK_QUEUE_SIZE, config, engine, true)
        .ifPresent(v -> config.setCpuLightQueueSize(v));

    resolveExpression(properties, IO_PREFIX + "." + THREAD_POOL_SIZE_CORE, config, engine, false)
        .ifPresent(v -> config.setIoCorePoolSize(v));
    resolveExpression(properties, IO_PREFIX + "." + THREAD_POOL_SIZE_MAX, config, engine, false)
        .ifPresent(v -> config.setIoMaxPoolSize(v));
    resolveExpression(properties, IO_PREFIX + "." + WORK_QUEUE_SIZE, config, engine, true)
        .ifPresent(v -> config.setIoQueueSize(v));
    resolveNumber(properties, IO_PREFIX + "." + THREAD_POOL_KEEP_ALIVE, true)
        .ifPresent(v -> config.setIoKeepAlive(v));

    resolveExpression(properties, CPU_INTENSIVE_PREFIX + "." + THREAD_POOL_SIZE, config, engine, false)
        .ifPresent(v -> config.setCpuIntensivePoolSize(v));
    resolveExpression(properties, CPU_INTENSIVE_PREFIX + "." + WORK_QUEUE_SIZE, config, engine, true)
        .ifPresent(v -> config.setCpuIntensiveQueueSize(v));

    return config;
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

  private static OptionalInt resolveExpression(Properties properties, String propName,
                                               ContainerThreadPoolsConfig threadPoolsConfig,
                                               ScriptEngine engine, boolean allowZero)
      throws MuleException {
    String property = null;

    final String sysPropOverride = System.getProperty(propName);
    if (sysPropOverride != null) {
      LOGGER.debug("Found system property override for '{}' with value '{}'", propName, sysPropOverride);

      property = sysPropOverride.trim().toLowerCase();
    } else if (properties.containsKey(propName)) {
      property = properties.getProperty(propName).trim().toLowerCase();
    }

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

  private static int cores = getRuntime().availableProcessors();
  private static long mem = getRuntime().maxMemory() / 1024;

  private long gracefulShutdownTimeout = 15000;
  private int cpuLightQueueSize = 0;
  private int cpuLightPoolSize = 2 * cores;
  private int ioQueueSize = 0;
  private int ioCorePoolSize = cores;
  private int ioMaxPoolSize = (int) max(2, cores + ((mem - 245760) / 5120));
  private long ioKeepAlive = 30000;
  private int cpuIntensiveQueueSize = 2 * cores;
  private int cpuIntensivePoolSize = 2 * cores;

  private ContainerThreadPoolsConfig() {

  }

  @Override
  public OptionalLong getGracefulShutdownTimeout() {
    return OptionalLong.of(gracefulShutdownTimeout);
  }

  private void setGracefulShutdownTimeout(long gracefulShutdownTimeout) {
    this.gracefulShutdownTimeout = gracefulShutdownTimeout;
  }

  @Override
  public OptionalInt getCpuLightPoolSize() {
    return OptionalInt.of(cpuLightPoolSize);
  }

  private void setCpuLightPoolSize(int cpuLightPoolSize) {
    this.cpuLightPoolSize = cpuLightPoolSize;
  }

  @Override
  public OptionalInt getCpuLightQueueSize() {
    return OptionalInt.of(cpuLightQueueSize);
  }

  private void setCpuLightQueueSize(int cpuLightQueueSize) {
    this.cpuLightQueueSize = cpuLightQueueSize;
  }

  @Override
  public OptionalInt getIoCorePoolSize() {
    return OptionalInt.of(ioCorePoolSize);
  }

  private void setIoCorePoolSize(int ioCorePoolSize) {
    this.ioCorePoolSize = ioCorePoolSize;
  }

  @Override
  public OptionalInt getIoMaxPoolSize() {
    return OptionalInt.of(max(ioCorePoolSize, ioMaxPoolSize));
  }

  private void setIoMaxPoolSize(int ioMaxPoolSize) {
    this.ioMaxPoolSize = ioMaxPoolSize;
  }

  @Override
  public OptionalInt getIoQueueSize() {
    return OptionalInt.of(ioQueueSize);
  }

  private void setIoQueueSize(int ioQueueSize) {
    this.ioQueueSize = ioQueueSize;
  }

  @Override
  public OptionalLong getIoKeepAlive() {
    return OptionalLong.of(ioKeepAlive);
  }

  private void setIoKeepAlive(long ioKeepAlive) {
    this.ioKeepAlive = ioKeepAlive;
  }

  @Override
  public OptionalInt getCpuIntensivePoolSize() {
    return OptionalInt.of(cpuIntensivePoolSize);
  }

  private void setCpuIntensivePoolSize(int cpuIntensivePoolSize) {
    this.cpuIntensivePoolSize = cpuIntensivePoolSize;
  }

  @Override
  public OptionalInt getCpuIntensiveQueueSize() {
    return OptionalInt.of(cpuIntensiveQueueSize);
  }

  private void setCpuIntensiveQueueSize(int cpuIntensiveQueueSize) {
    this.cpuIntensiveQueueSize = cpuIntensiveQueueSize;
  }

  @Override
  public String getThreadNamePrefix() {
    return "[MuleRuntime].";
  }
}
