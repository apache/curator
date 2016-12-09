package org.apache.curator.framework.recipes.nodes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.concurrent.TimeUnit;

public class TestPersistentTtlNode extends BaseClassForTests
{
    private final Timing timing = new Timing();

    @BeforeMethod
    @Override
    public void setup() throws Exception
    {
        System.setProperty("znode.container.checkIntervalMs", "1");
        super.setup();
    }

    @AfterMethod
    @Override
    public void teardown() throws Exception
    {
        System.clearProperty("znode.container.checkIntervalMs");
        super.teardown();
    }

    @Test
    public void testBasic() throws Exception
    {
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1)))
        {
            client.start();

            try (PersistentTtlNode node = new PersistentTtlNode(client, "/test", 10, new byte[0]))
            {
                node.start();
                node.waitForInitialCreate(timing.session(), TimeUnit.MILLISECONDS);

                for ( int i = 0; i < 10; ++i )
                {
                    Thread.sleep(10);
                    Assert.assertNotNull(client.checkExists().forPath("/test"));
                }
            }

            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/test"));
        }
    }
}
