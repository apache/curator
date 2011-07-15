package com.netflix.curator.framework;

import com.netflix.curator.utils.TestingServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseClassForTests
{
    protected TestingServer server;

    @BeforeMethod
    public void     setup() throws Exception
    {
        server = new TestingServer();
    }

    @AfterMethod
    public void     teardown() throws InterruptedException
    {
        server.close();
    }
}
