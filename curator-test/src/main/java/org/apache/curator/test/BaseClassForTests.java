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

import com.google.common.io.Closeables;
import java.io.Closeable;
import java.net.BindException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IRetryAnalyzer;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;

public class BaseClassForTests
{
    private static final Logger log = LoggerFactory.getLogger(BaseClassForTests.class);

    protected TestingServer server;
    protected Timing timing;

    private static final int    RETRY_WAIT_MS = 5000;
    private static final String INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES;
    private static final String INTERNAL_RETRY_FAILED_TESTS;
    static
    {
        String logConnectionIssues = null;
        String retryFailedTests = null;
        try
        {
            // use reflection to avoid adding a circular dependency in the pom
            Class<?> debugUtilsClazz = Class.forName("org.apache.curator.utils.DebugUtils");
            logConnectionIssues = (String)debugUtilsClazz.getField("PROPERTY_DONT_LOG_CONNECTION_ISSUES").get(null);
            retryFailedTests = (String)debugUtilsClazz.getField("PROPERTY_RETRY_FAILED_TESTS").get(null);
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES = logConnectionIssues;
        INTERNAL_RETRY_FAILED_TESTS = retryFailedTests;
    }

    private List<Closeable> toClose;

    @BeforeSuite(alwaysRun = true)
    public void beforeSuite(ITestContext context)
    {
        for ( ITestNGMethod method : context.getAllTestMethods() )
        {
            method.setRetryAnalyzer(new RetryTest());
        }
    }

    @BeforeMethod
    public void setup() throws Exception
    {
        timing = new Timing();
        toClose = new ArrayList<Closeable>();

        if ( INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES != null )
        {
            System.setProperty(INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES, "true");
        }

        while ( server == null )
        {
            try
            {
                server = autoClose(new TestingServer());
            }
            catch ( BindException e )
            {
                log.error("Getting bind exception - retrying to allocate server");
                server = null;
            }
        }
    }

    @AfterMethod
    public void teardown() throws Exception
    {
        for ( Closeable closeable : toClose )
        {
            // Don't need closeQuietly because we don't care that it advertises a throw
            Closeables.close(closeable, true);
        }
    }

    /**
     * Ask the test framework to automatically close the given resource after the test.
     * <p>
     * Sample usage: <tt>CuratorFramework client =
     * autoClose(CuratorFrameworkFactory.newClient(...))</tt>
     *
     * @return The given resource, for fluent-style invocation
     */
    protected <T extends Closeable> T autoClose(T closeable)
    {
        if ( closeable != null )
        {
            toClose.add(closeable);
        }
        return closeable;
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
                log.error("Waiting %d ms and retrying test. Name: %s - TestName: %s ", RETRY_WAIT_MS, result.getName(), result.getTestName());
                try
                {
                    Thread.sleep(RETRY_WAIT_MS);
                }
                catch ( InterruptedException e )
                {
                    log.error("Retry interrupted. Name: %s - TestName: %s ", result.getName(), result.getTestName());
                    Thread.currentThread().interrupt();
                    isRetrying = false;
                }
            }
            return isRetrying;
        }
    }

}
