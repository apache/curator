package org.apache.curator.framework.imps;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRemoveWatches extends BaseClassForTests
{
    @Test
    public void testRemoveCuratorWatch() throws Exception
    {       
        CuratorFramework client = CuratorFrameworkFactory.builder().
                connectString(server.getConnectString()).
                retryPolicy(new RetryOneTime(1)).
                build();
        try
        {
            client.start();
            
            CuratorWatcher watcher = new CuratorWatcher()
            {
                
                @Override
                public void process(WatchedEvent event) throws Exception
                {
                    // TODO Auto-generated method stub
                    
                }
            };
            
            String path = "/";
            client.checkExists().usingWatcher(watcher).forPath(path);
            
            client.removeWatches().watcher(watcher).ofType(WatcherType.Any).forPath(path);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }    
    
    @Test
    public void testRemoveWatch() throws Exception
    {       
        CuratorFramework client = CuratorFrameworkFactory.builder().
                connectString(server.getConnectString()).
                retryPolicy(new RetryOneTime(1)).
                build();
        try
        {
            client.start();
            
            Watcher watcher = new Watcher()
            {                
                @Override
                public void process(WatchedEvent event)
                {
                }
            };
            
            String path = "/";
            client.checkExists().usingWatcher(watcher).forPath(path);
            
            client.removeWatches().watcher(watcher).ofType(WatcherType.Any).forPath(path);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
    
    @Test
    public void testRemoveWatchInBackgroundWithCallback() throws Exception
    {       
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder().
                connectString(server.getConnectString()).
                retryPolicy(new RetryOneTime(1)).
                build();
        try
        {
            client.start();
            
            Watcher watcher = new Watcher()
            {                
                @Override
                public void process(WatchedEvent event)
                {
                }
            };

            final CountDownLatch removedLatch = new CountDownLatch(1);
            BackgroundCallback callback = new BackgroundCallback()
            {
                
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event)
                        throws Exception
                {
                    removedLatch.countDown();
                }
            };
            
            String path = "/";
            client.checkExists().usingWatcher(watcher).forPath(path);
            
            client.removeWatches().watcher(watcher).ofType(WatcherType.Any).inBackground(callback).forPath(path);
            
            Assert.assertTrue(timing.awaitLatch(removedLatch), "Timed out waiting for watch removal");
            
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
    
    @Test
    public void testRemoveWatchInBackgroundWithNoCallback() throws Exception
    {       
        Timing timing = new Timing();
        CuratorFramework client = CuratorFrameworkFactory.builder().
                connectString(server.getConnectString()).
                retryPolicy(new RetryOneTime(1)).
                build();
        try
        {
            client.start();
            
            Watcher watcher = new Watcher()
            {                
                @Override
                public void process(WatchedEvent event)
                {
                }
            };

            final CountDownLatch removedLatch = new CountDownLatch(1);
            client.getCuratorListenable().addListener(new CuratorListener()
            {
                
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event)
                        throws Exception
                {
                    removedLatch.countDown();
                }
            });
            
            String path = "/";
            client.checkExists().usingWatcher(watcher).forPath(path);
            
            client.removeWatches().watcher(watcher).ofType(WatcherType.Any).inBackground().forPath(path);
            
            Assert.assertTrue(timing.awaitLatch(removedLatch), "Timed out waiting for watch removal");
            
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }        
    
    @Test
    public void testRemoveAllWatches() throws Exception
    {       
        CuratorFramework client = CuratorFrameworkFactory.builder().
                connectString(server.getConnectString()).
                retryPolicy(new RetryOneTime(1)).
                build();
        try
        {
            client.start();
            
            Watcher watcher1 = new Watcher()
            {                
                @Override
                public void process(WatchedEvent event)
                {
                }
            };
            
            Watcher watcher2 = new Watcher()
            {                
                @Override
                public void process(WatchedEvent event)
                {
                }
            };            
            
            String path = "/";
            client.checkExists().usingWatcher(watcher1).forPath(path);
            client.checkExists().usingWatcher(watcher2).forPath(path);
            
            client.removeWatches().allWatches().ofType(WatcherType.Any).forPath(path);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }    
}
