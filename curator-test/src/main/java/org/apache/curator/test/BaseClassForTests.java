package org.apache.curator.test;

import org.testng.IRetryAnalyzer;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import java.net.BindException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BaseClassForTests
{
    protected TestingServer server;

    private static final int    RETRY_WAIT_MS = 5000;
    private static final String INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES;
    static
    {
        String s = null;
        try
        {
            // use reflection to avoid adding a circular dependency in the pom
            s = (String)Class.forName("org.apache.curator.utils.DebugUtils").getField("PROPERTY_DONT_LOG_CONNECTION_ISSUES").get(null);
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        INTERNAL_PROPERTY_DONT_LOG_CONNECTION_ISSUES = s;
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
        server.close();
        server = null;
    }

    private static class RetryTest implements IRetryAnalyzer
    {
        private final AtomicBoolean hasBeenRetried = new AtomicBoolean(false);

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
