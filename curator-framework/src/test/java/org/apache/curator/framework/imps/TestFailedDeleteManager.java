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
package org.apache.curator.framework.imps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestFailedDeleteManager extends BaseClassForTests
{
    @Test
    public void     testLostSession() throws Exception
    {
        Timing                  timing = new Timing();
        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new ExponentialBackoffRetry(100, 3));
        try
        {
            client.start();

            client.create().forPath("/test-me");

            final CountDownLatch            latch = new CountDownLatch(1);
            final Semaphore                 semaphore = new Semaphore(0);
            ConnectionStateListener         listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( (newState == ConnectionState.LOST) || (newState == ConnectionState.SUSPENDED) )
                    {
                        semaphore.release();
                    }
                    else if ( newState == ConnectionState.RECONNECTED )
                    {
                        latch.countDown();
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);
            server.stop();

            Assert.assertTrue(timing.acquireSemaphore(semaphore));
            try
            {
                client.delete().guaranteed().forPath("/test-me");
                Assert.fail();
            }
            catch ( KeeperException.ConnectionLossException e )
            {
                // expected
            }
            Assert.assertTrue(timing.acquireSemaphore(semaphore));

            timing.sleepABit();

            server.restart();
            Assert.assertTrue(timing.awaitLatch(latch));

            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/test-me"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testWithNamespaceAndLostSession() throws Exception
    {
        Timing                  timing = new Timing();
        CuratorFramework        client = CuratorFrameworkFactory.builder().connectString(server.getConnectString())
            .sessionTimeoutMs(timing.session())
            .connectionTimeoutMs(timing.connection())
            .retryPolicy(new ExponentialBackoffRetry(100, 3))
            .namespace("aisa")
            .build();
        try
        {
            client.start();

            client.create().forPath("/test-me");

            final CountDownLatch            latch = new CountDownLatch(1);
            final Semaphore                 semaphore = new Semaphore(0);
            ConnectionStateListener         listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( (newState == ConnectionState.LOST) || (newState == ConnectionState.SUSPENDED) )
                    {
                        semaphore.release();
                    }
                    else if ( newState == ConnectionState.RECONNECTED )
                    {
                        latch.countDown();
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);
            server.stop();

            Assert.assertTrue(timing.acquireSemaphore(semaphore));
            try
            {
                client.delete().guaranteed().forPath("/test-me");
                Assert.fail();
            }
            catch ( KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e )
            {
                // expected
            }
            Assert.assertTrue(timing.acquireSemaphore(semaphore));

            timing.sleepABit();

            server.restart();
            Assert.assertTrue(timing.awaitLatch(latch));

            timing.sleepABit();

            Assert.assertNull(client.checkExists().forPath("/test-me"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testWithNamespaceAndLostSessionAlt() throws Exception
    {
        Timing                  timing = new Timing();
        CuratorFramework        client = CuratorFrameworkFactory.builder().connectString(server.getConnectString())
            .sessionTimeoutMs(timing.session())
            .connectionTimeoutMs(timing.connection())
            .retryPolicy(new ExponentialBackoffRetry(100, 3))
            .build();
        try
        {
            client.start();

            CuratorFramework        namespaceClient = client.usingNamespace("foo");
            namespaceClient.create().forPath("/test-me");

            final CountDownLatch            latch = new CountDownLatch(1);
            final Semaphore                 semaphore = new Semaphore(0);
            ConnectionStateListener         listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( (newState == ConnectionState.LOST) || (newState == ConnectionState.SUSPENDED) )
                    {
                        semaphore.release();
                    }
                    else if ( newState == ConnectionState.RECONNECTED )
                    {
                        latch.countDown();
                    }
                }
            };
            namespaceClient.getConnectionStateListenable().addListener(listener);
            server.stop();

            Assert.assertTrue(timing.acquireSemaphore(semaphore));
            try
            {
                namespaceClient.delete().guaranteed().forPath("/test-me");
                Assert.fail();
            }
            catch ( KeeperException.ConnectionLossException e )
            {
                // expected
            }
            Assert.assertTrue(timing.acquireSemaphore(semaphore));

            timing.sleepABit();

            server.restart();
            Assert.assertTrue(timing.awaitLatch(latch));

            timing.sleepABit();

            Assert.assertNull(namespaceClient.checkExists().forPath("/test-me"));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void     testBasic() throws Exception
    {
        final String PATH = "/one/two/three";

        Timing                          timing = new Timing();
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder.connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).connectionTimeoutMs(timing.connection()).sessionTimeoutMs(timing.session());
        CuratorFrameworkImpl            client = new CuratorFrameworkImpl(builder);
        client.start();
        try
        {
            client.create().creatingParentsIfNeeded().forPath(PATH);
            Assert.assertNotNull(client.checkExists().forPath(PATH));

            server.stop(); // cause the next delete to fail
            try
            {
                client.delete().forPath(PATH);
                Assert.fail();
            }
            catch ( KeeperException.ConnectionLossException e )
            {
                // expected
            }
            
            server.restart();
            Assert.assertNotNull(client.checkExists().forPath(PATH));

            server.stop(); // cause the next delete to fail
            try
            {
                client.delete().guaranteed().forPath(PATH);
                Assert.fail();
            }
            catch ( KeeperException.ConnectionLossException e )
            {
                // expected
            }

            server.restart();

            final int       TRIES = 5;
            for ( int i = 0; i < TRIES; ++i )
            {
                if ( client.checkExists().forPath(PATH) != null )
                {
                    timing.sleepABit();
                }
            }
            Assert.assertNull(client.checkExists().forPath(PATH));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
    
    @Test
    public void testGuaranteedDeleteOnNonExistentNodeInForeground() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        
        final AtomicBoolean pathAdded = new AtomicBoolean(false);
        
        ((CuratorFrameworkImpl)client).getFailedDeleteManager().debugListener = new FailedOperationManager.FailedOperationManagerListener<String>()
        {
            
            @Override
            public void pathAddedForGuaranteedOperation(String path)
            {
                pathAdded.set(true);
            }
        };
        
        try
        {
            client.delete().guaranteed().forPath("/nonexistent");
            Assert.fail();
        }
        catch(NoNodeException e)
        {
            //Exception is expected, the delete should not be retried
            Assert.assertFalse(pathAdded.get());
        }
        finally
        {
            client.close();
        }        
    }
    
    @Test
    public void testGuaranteedDeleteOnNonExistentNodeInBackground() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        
        final AtomicBoolean pathAdded = new AtomicBoolean(false);
        
        ((CuratorFrameworkImpl)client).getFailedDeleteManager().debugListener = new FailedOperationManager.FailedOperationManagerListener<String>()
        {
            
            @Override
            public void pathAddedForGuaranteedOperation(String path)
            {
                pathAdded.set(true);
            }
        };
        
        final CountDownLatch backgroundLatch = new CountDownLatch(1);
        
        BackgroundCallback background = new BackgroundCallback()
        {
            
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event)
                    throws Exception
            {
                backgroundLatch.countDown();
            }
        };
        
        try
        {
            client.delete().guaranteed().inBackground(background).forPath("/nonexistent");
            
            backgroundLatch.await();
            
            //Exception is expected, the delete should not be retried
            Assert.assertFalse(pathAdded.get());
        }
        finally
        {
            client.close();
        }        
    }    
}
