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

import org.testng.IRetryAnalyzer;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import java.io.IOException;
import java.net.BindException;

public class BaseClassForTests
{
    protected TestingServer server;

    private static final String INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES;
    static
    {
        String logConnectionIssues = null;
        String retryFailedTests = null;
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
    }

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
        if ( INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES != null )
        {
            System.setProperty(INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES, "true");
        }

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

    private static class RetryTest implements IRetryAnalyzer
    {
        private boolean wasRetried = false;

        @Override
        public boolean retry(ITestResult result)
        {
            if ( result.isSuccess() || wasRetried )
            {
                wasRetried = false;
                return false;
            }

            wasRetried = true;
            System.err.println(String.format("Retry test 1 time. Name: %s - TestName: %s ", result.getName(), result.getTestName()));
            return true;
        }
    }

}
