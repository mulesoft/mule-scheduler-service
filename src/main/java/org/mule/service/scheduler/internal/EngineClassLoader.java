/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.service.scheduler.internal;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

public class EngineClassLoader extends URLClassLoader {

  public EngineClassLoader() throws MalformedURLException {
    super(new URL[0], createOptClassloader(emptyList()));

    List<File> files =
        listJars(new File("/Users/ayelen.lopez/Mulesoft/standalones/mule-enterprise-standalone-4.6.0-SNAPSHOT_ScriptEngine/engine-lib"));

    for (File file : files) {
      addURL(file.getAbsoluteFile().toURI().toURL());
    }
  }

  private static ClassLoader createOptClassloader(List<URL> optUrls) {
    return new URLClassLoader(optUrls.toArray(new URL[optUrls.size()]));
  }

  protected List<File> listJars(File path) {
    File[] jars = path.listFiles(pathname -> {
      try {
        return pathname.getCanonicalPath().endsWith(".jar");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    if (jars != null) {
      return asList(jars);
    }
    return emptyList();
  }
}
