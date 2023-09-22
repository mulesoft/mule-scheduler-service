/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
import org.mule.api.annotation.jpms.ServiceModule;
import org.mule.api.annotation.jpms.ServiceModule.RequiredOpens;

/**
 * Mule Scheduler Service Implementation.
 *
 * @moduleGraph
 * @since 1.5
 */
@ServiceModule(
    requiredOpens = {
        // required for cleaning up the thread localas after a task finishes execution
        @RequiredOpens(
            moduleName = "java.base",
            packageNames = {
                "java.lang"
            })
    })
module org.mule.service.scheduler {

  requires org.mule.runtime.api;
  requires org.mule.runtime.profiling.api;
  // context injection
  requires org.mule.runtime.core;

  requires java.inject;

  requires quartz;

  requires org.graalvm.js.scriptengine;
  requires java.scripting;

  requires com.github.benmanes.caffeine;
  requires org.apache.commons.lang3;

  // Allow invocation and injection into providers by the Mule Runtime
  exports org.mule.service.scheduler.provider to
      org.mule.runtime.service;
  exports org.mule.service.scheduler.internal.service to
      org.mule.runtime.service;

  exports org.mule.service.scheduler.internal to
      quartz;
}
