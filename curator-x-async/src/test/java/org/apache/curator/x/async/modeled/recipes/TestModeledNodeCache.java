package org.apache.curator.x.async.modeled.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestModeledNodeCache extends BaseClassForTests
{
    private static final Timing timing = new Timing();
    private CuratorFramework client;

    @BeforeMethod
    @Override
    public void setup() throws Exception
    {
        super.setup();

        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
    }

    @AfterMethod
    @Override
    public void teardown() throws Exception
    {
        CloseableUtils.closeQuietly(client);

        super.teardown();
    }

    @Test
    public void testBasic()
    {
        ModeledNodeCache nodeCache = null;//ModeledNodeCache.wrap(new NodeCache(client, modeledDetails.getPath().fullPath()));
        nodeCache.start();
    }
}
