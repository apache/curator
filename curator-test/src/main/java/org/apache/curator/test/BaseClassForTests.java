/**
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener2;
import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import java.io.IOException;
import java.net.BindException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BaseClassForTests
{
    protected volatile TestingServer server;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AtomicBoolean isRetrying = new AtomicBoolean(false);

    private static final String INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES;
    private static final String INTERNAL_PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND;
    private static final String INTERNAL_PROPERTY_VALIDATE_NAMESPACE_WATCHER_MAP_EMPTY;

    static
    {
        String logConnectionIssues = null;
        try
        {
            // use reflection to avoid adding a circular dependency in the pom
            Class<?> debugUtilsClazz = Class.forName("org.apache.curator.utils.DebugUtils");
            logConnectionIssues = (String)debugUtilsClazz.getField("PROPERTY_DONT_LOG_CONNECTION_ISSUES").get(null);
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES = logConnectionIssues;
        String s = null;
        try
        {
            // use reflection to avoid adding a circular dependency in the pom
            s = (String)Class.forName("org.apache.curator.utils.DebugUtils").getField("PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND").get(null);
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        INTERNAL_PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND = s;
        s = null;
        try
        {
            // use reflection to avoid adding a circular dependency in the pom
            s = (String)Class.forName("org.apache.curator.utils.DebugUtils").getField("PROPERTY_VALIDATE_NAMESPACE_WATCHER_MAP_EMPTY").get(null);
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        INTERNAL_PROPERTY_VALIDATE_NAMESPACE_WATCHER_MAP_EMPTY = s;
    }

    @BeforeSuite(alwaysRun = true)
    public void beforeSuite(ITestContext context)
    {
        IInvokedMethodListener2 methodListener2 = new IInvokedMethodListener2()
        {
            @Override
            public void beforeInvocation(IInvokedMethod method, ITestResult testResult)
            {
                method.getTestMethod().setRetryAnalyzer(BaseClassForTests.this::retry);
            }

            @Override
            public void beforeInvocation(IInvokedMethod method, ITestResult testResult, ITestContext context)
            {
                beforeInvocation(method, testResult);
            }

            @Override
            public void afterInvocation(IInvokedMethod method, ITestResult testResult, ITestContext context)
            {
                // NOP
            }

            @Override
            public void afterInvocation(IInvokedMethod method, ITestResult testResult)
            {
                // NOP
            }
        };
        context.getSuite().addListener(methodListener2);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception
    {
        if ( INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES != null )
        {
            System.setProperty(INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES, "true");
        }
        System.setProperty(INTERNAL_PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND, "true");
        System.setProperty(INTERNAL_PROPERTY_VALIDATE_NAMESPACE_WATCHER_MAP_EMPTY, "true");

        try
        {
            createServer();
        }
        catch ( FailedServerStartException ignore )
        {
            log.warn("Failed to start server - retrying 1 more time");
            // server creation failed - we've sometime seen this with re-used addresses, etc. - retry one more time
            closeServer();
            createServer();
        }
    }

    protected void createServer() throws Exception
    {
        while ( server == null )
        {
            try
            {
                server = new TestingServer();
            }
            catch ( BindException e )
            {
                server = null;
                throw new FailedServerStartException("Getting bind exception - retrying to allocate server");
            }
        }
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception
    {
        System.clearProperty(INTERNAL_PROPERTY_VALIDATE_NAMESPACE_WATCHER_MAP_EMPTY);
        System.clearProperty(INTERNAL_PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND);
        closeServer();
    }

    private void closeServer()
    {
        if ( server != null )
        {
            try
            {
                server.close();
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
            finally
            {
                server = null;
            }
        }
    }

    private boolean retry(ITestResult result)
    {
        if ( result.isSuccess() || isRetrying.get() )
        {
            isRetrying.set(false);
            return false;
        }

        result.setStatus(ITestResult.SKIP);
        if ( result.getThrowable() != null )
        {
            log.error("Retrying 1 time", result.getThrowable());
        }
        else
        {
            log.error("Retrying 1 time");
        }
        isRetrying.set(true);
        return true;
    }
}
