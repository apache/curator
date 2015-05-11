package org.apache.curator.framework.imps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.test.WatchersDebug;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;

public class TestWatcherRemovalManager extends BaseClassForTests
{
    @Test
    public void testBasic() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            WatcherRemoveCuratorFramework removerClient = client.newWatcherRemoveCuratorFramework();

            Watcher watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    // NOP
                }
            };
            removerClient.checkExists().usingWatcher(watcher).forPath("/hey");

            List<String> existWatches = WatchersDebug.getExistWatches(client.getZookeeperClient().getZooKeeper());
            Assert.assertEquals(existWatches.size(), 1);

            removerClient.removeWatchers();

            new Timing().sleepABit();

            existWatches = WatchersDebug.getExistWatches(client.getZookeeperClient().getZooKeeper());
            Assert.assertEquals(existWatches.size(), 0);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
}
