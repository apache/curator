package com.netflix.curator.framework.recipes.locks;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.BaseClassForTests;
import com.netflix.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestInterProcessSemaphore extends BaseClassForTests
{
    @Test
    public void     testSimple() throws Exception
    {
        final int       MAX_LEASES = 3;

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        try
        {
            List<InterProcessSemaphore>        leases = Lists.newArrayList();
            for ( int i = 0; i < MAX_LEASES; ++i )
            {
                InterProcessSemaphore      semaphore = new InterProcessSemaphore(client, "/test", MAX_LEASES);
                Assert.assertTrue(semaphore.acquire(3, TimeUnit.SECONDS));
                leases.add(semaphore);
            }

            InterProcessSemaphore      semaphore = new InterProcessSemaphore(client, "/test", MAX_LEASES);
            Assert.assertFalse(semaphore.acquire(1, TimeUnit.SECONDS));

            leases.remove(0).release();
            Assert.assertNotNull(semaphore.acquire(3, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }
}
