/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.test;

import java.io.IOException;
import java.net.BindException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseClassForTests {
    protected volatile TestingServer server;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AtomicBoolean isRetrying = new AtomicBoolean(false);

    private static final String INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES;
    private static final String INTERNAL_PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND;
    private static final String INTERNAL_PROPERTY_VALIDATE_NAMESPACE_WATCHER_MAP_EMPTY;

    static {
        String logConnectionIssues = null;
        try {
            // use reflection to avoid adding a circular dependency in the pom
            Class<?> debugUtilsClazz = Class.forName("org.apache.curator.utils.DebugUtils");
            logConnectionIssues = (String) debugUtilsClazz
                    .getField("PROPERTY_DONT_LOG_CONNECTION_ISSUES")
                    .get(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES = logConnectionIssues;
        String s = null;
        try {
            // use reflection to avoid adding a circular dependency in the pom
            s = (String) Class.forName("org.apache.curator.utils.DebugUtils")
                    .getField("PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND")
                    .get(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        INTERNAL_PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND = s;
        s = null;
        try {
            // use reflection to avoid adding a circular dependency in the pom
            s = (String) Class.forName("org.apache.curator.utils.DebugUtils")
                    .getField("PROPERTY_VALIDATE_NAMESPACE_WATCHER_MAP_EMPTY")
                    .get(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        INTERNAL_PROPERTY_VALIDATE_NAMESPACE_WATCHER_MAP_EMPTY = s;
    }

    @BeforeEach
    public void setup() throws Exception {
        if (INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES != null) {
            System.setProperty(INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES, "true");
        }
        System.setProperty(INTERNAL_PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND, "true");
        System.setProperty(INTERNAL_PROPERTY_VALIDATE_NAMESPACE_WATCHER_MAP_EMPTY, "true");

        try {
            createServer();
        } catch (FailedServerStartException ignore) {
            log.warn("Failed to start server - retrying 1 more time");
            // server creation failed - we've sometime seen this with re-used addresses, etc. - retry one more time
            closeServer();
            createServer();
        }
    }

    public TestingCluster createAndStartCluster(int qty) throws Exception {
        TestingCluster cluster = new TestingCluster(qty);
        try {
            cluster.start();
        } catch (FailedServerStartException e) {
            log.warn("Failed to start cluster - retrying 1 more time");
            // cluster creation failed - we've sometime seen this with re-used addresses, etc. - retry one more time
            try {
                cluster.close();
            } catch (Exception ex) {
                // ignore
            }
            cluster = new TestingCluster(qty);
            cluster.start();
        }
        return cluster;
    }

    protected void createServer() throws Exception {
        while (server == null) {
            try {
                server = new TestingServer();
            } catch (BindException e) {
                server = null;
                throw new FailedServerStartException("Getting bind exception - retrying to allocate server");
            }
        }
    }

    protected void restartServer() throws Exception {
        if (server != null) {
            server.restart();
        }
    }

    @AfterEach
    public void teardown() throws Exception {
        System.clearProperty(INTERNAL_PROPERTY_VALIDATE_NAMESPACE_WATCHER_MAP_EMPTY);
        System.clearProperty(INTERNAL_PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND);
        closeServer();
    }

    private void closeServer() {
        if (server != null) {
            try {
                server.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                server = null;
            }
        }
    }
}
