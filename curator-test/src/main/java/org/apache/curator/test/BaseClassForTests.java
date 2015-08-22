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
import org.testng.IInvokedMethodListener;
import org.testng.IRetryAnalyzer;
import org.testng.ITestContext;
import org.testng.ITestNGListener;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import java.io.IOException;
import java.net.BindException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BaseClassForTests
{
    protected TestingServer server;
    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final int RETRY_WAIT_MS = 5000;
    private static final String INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES;
    private static final String INTERNAL_PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND;
    private static final String INTERNAL_RETRY_FAILED_TESTS;
    private static final String INTERNAL_CHECK_INJECTED_DEBUG_EXCEPTIONS;

    static
    {
        String logConnectionIssues = null;
        String retryFailedTests = null;
        String checkInjectedDebugExceptions = null;
        try
        {
            // use reflection to avoid adding a circular dependency in the pom
            Class<?> debugUtilsClazz = Class.forName("org.apache.curator.utils.DebugUtils");
            logConnectionIssues = (String)debugUtilsClazz.getField("PROPERTY_DONT_LOG_CONNECTION_ISSUES").get(null);
            retryFailedTests = (String)debugUtilsClazz.getField("PROPERTY_RETRY_FAILED_TESTS").get(null);
            checkInjectedDebugExceptions = (String)debugUtilsClazz.getField("PROPERTY_CHECK_INJECTED_DEBUG_EXCEPTIONS").get(null);
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES = logConnectionIssues;
        INTERNAL_RETRY_FAILED_TESTS = retryFailedTests;
        INTERNAL_CHECK_INJECTED_DEBUG_EXCEPTIONS = checkInjectedDebugExceptions;
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
    }

    @BeforeSuite(alwaysRun = true)
    public void beforeSuite(ITestContext context)
    {
        if ( !enabledSessionExpiredStateAware() )
        {
            ITestNGListener listener = new IInvokedMethodListener()
            {
                @Override
                public void beforeInvocation(IInvokedMethod method, ITestResult testResult)
                {
                    int invocationCount = method.getTestMethod().getCurrentInvocationCount();
                    System.setProperty("curator-enable-session-expired-state", Boolean.toString(invocationCount == 1));
                    log.info("curator-enable-session-expired-state: " + Boolean.toString(invocationCount == 1));
                }

                @Override
                public void afterInvocation(IInvokedMethod method, ITestResult testResult)
                {
                    System.clearProperty("curator-enable-session-expired-state");
                }
            };
            context.getSuite().addListener(listener);
        }

        for ( ITestNGMethod method : context.getAllTestMethods() )
        {
            method.setInvocationCount(enabledSessionExpiredStateAware() ? 1 : 2);
            method.setRetryAnalyzer(new RetryTest());
        }
    }

    @BeforeMethod
    public void setup() throws Exception
    {
        if ( INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES != null )
        {
            System.setProperty(INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES, "true");
        }
        System.setProperty(INTERNAL_PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND, "true");
        System.setProperty(INTERNAL_CHECK_INJECTED_DEBUG_EXCEPTIONS, "true");

        while ( server == null )
        {
            try
            {
                server = new TestingServer();
            }
            catch ( BindException e )
            {
                System.err.println("Getting bind exception - retrying to allocate server");
                server = null;
            }
        }
    }

    @AfterMethod
    public void teardown() throws Exception
    {
        System.clearProperty(INTERNAL_PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND);
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
            server = null;
        }
    }

    protected boolean enabledSessionExpiredStateAware()
    {
        return false;
    }

    private static class RetryTest implements IRetryAnalyzer
    {
        private final AtomicBoolean hasBeenRetried = new AtomicBoolean(!Boolean.getBoolean(INTERNAL_RETRY_FAILED_TESTS));

        @Override
        public boolean retry(ITestResult result)
        {
            boolean isRetrying = hasBeenRetried.compareAndSet(false, true);
            if ( isRetrying )
            {
                System.err.println(String.format("Waiting " + RETRY_WAIT_MS + " ms and retrying test. Name: %s - TestName: %s ", result.getName(), result.getTestName()));
                try
                {
                    Thread.sleep(RETRY_WAIT_MS);
                }
                catch ( InterruptedException e )
                {
                    System.err.println(String.format("Retry interrupted. Name: %s - TestName: %s ", result.getName(), result.getTestName()));
                    Thread.currentThread().interrupt();
                    isRetrying = false;
                }
            }
            return isRetrying;
        }
    }

}
