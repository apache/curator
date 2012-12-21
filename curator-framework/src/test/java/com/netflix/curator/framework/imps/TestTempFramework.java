package com.netflix.curator.framework.imps;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.CuratorTempFramework;
import com.netflix.curator.retry.RetryOneTime;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestTempFramework extends BaseClassForTests
{
    @Test
    public void testBasic() throws Exception
    {
        CuratorTempFramework        client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).buildTemp();
        try
        {
            client.inTransaction().create().forPath("/foo", "data".getBytes()).and().commit();

            byte[] bytes = client.getData().forPath("/foo");
            Assert.assertEquals(bytes, "data".getBytes());
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void testInactivity() throws Exception
    {
        final CuratorTempFrameworkImpl        client = (CuratorTempFrameworkImpl)CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).buildTemp(1000);
        try
        {
            ScheduledExecutorService    service = Executors.newScheduledThreadPool(1);
            Runnable                    command = new Runnable()
            {
                @Override
                public void run()
                {
                    client.updateLastAccess();
                }
            };
            service.scheduleAtFixedRate(command, 10, 10, TimeUnit.MILLISECONDS);
            client.inTransaction().create().forPath("/foo", "data".getBytes()).and().commit();
            service.shutdownNow();
            Thread.sleep(2000);

            Assert.assertNull(client.getCleanup());
            Assert.assertNull(client.getClient());
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }
}
