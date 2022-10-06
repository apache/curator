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
package org.apache.curator.framework.recipes.barriers;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

public class TestDistributedBarrier extends BaseClassForTests
{
    @Test
    public void     testServerCrash() throws Exception
    {
        final int                         TIMEOUT = 1000;

        final CuratorFramework            client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).connectionTimeoutMs(TIMEOUT).retryPolicy(new RetryOneTime(1)).build();
        try
        {
            client.start();

            final DistributedBarrier      barrier = new DistributedBarrier(client, "/barrier");
            barrier.setBarrier();

            final ExecutorService        service = Executors.newSingleThreadExecutor();
            Future<Object>               future = service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        Thread.sleep(TIMEOUT / 2);
                        server.stop();
                        return null;
                    }
                }
            );

            barrier.waitOnBarrier(TIMEOUT * 2, TimeUnit.SECONDS);
            future.get();
            fail();
        }
        catch ( KeeperException.ConnectionLossException expected )
        {
            // expected
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testMultiClient() throws Exception
    {
        CuratorFramework            client1 = null;
        CuratorFramework            client2 = null;
        try
        {
            {
                CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                try
                {
                    client.start();
                    DistributedBarrier      barrier = new DistributedBarrier(client, "/barrier");
                    barrier.setBarrier();
                }
                finally
                {
                    CloseableUtils.closeQuietly(client);
                }
            }

            client1 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client2 = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));

            List<Future<Object>>        futures = Lists.newArrayList();
            ExecutorService             service = Executors.newCachedThreadPool();
            for ( final CuratorFramework c : new CuratorFramework[]{client1, client2} )
            {
                Future<Object> future = service.submit
                (
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            c.start();
                            DistributedBarrier barrier = new DistributedBarrier(c, "/barrier");
                            barrier.waitOnBarrier(10, TimeUnit.MILLISECONDS);
                            return null;
                        }
                    }
                );
                futures.add(future);
            }

            Thread.sleep(1000);
            {
                CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
                try
                {
                    client.start();
                    DistributedBarrier      barrier = new DistributedBarrier(client, "/barrier");
                    barrier.removeBarrier();
                }
                finally
                {
                    CloseableUtils.closeQuietly(client);
                }
            }

            for ( Future<Object> f : futures )
            {
                f.get();
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(client1);
            CloseableUtils.closeQuietly(client2);
        }
    }

    @Test
    public void     testNoBarrier() throws Exception
    {
        CuratorFramework            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            final DistributedBarrier      barrier = new DistributedBarrier(client, "/barrier");
            assertTrue(barrier.waitOnBarrier(10, TimeUnit.SECONDS));

            // just for grins, test the infinite wait
            ExecutorService         service = Executors.newSingleThreadExecutor();
            Future<Object>          future = service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        barrier.waitOnBarrier();
                        return "";
                    }
                }
            );
            assertTrue(future.get(10, TimeUnit.SECONDS) != null);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testBasic() throws Exception
    {
        CuratorFramework            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            final DistributedBarrier      barrier = new DistributedBarrier(client, "/barrier");
            barrier.setBarrier();

            ExecutorService               service = Executors.newSingleThreadExecutor();
            service.submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        Thread.sleep(1000);
                        barrier.removeBarrier();
                        return null;
                    }
                }
            );

            assertTrue(barrier.waitOnBarrier(10, TimeUnit.SECONDS));
        }
        finally
        {
            client.close();
        }
    }

    @Test
    public void     testIsSet() throws Exception
    {
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();

            final DistributedBarrier barrier = new DistributedBarrier(client, "/barrier");
            barrier.setBarrier();

            assertTrue(barrier.isSet());
        }
    }

    @Test
    public void     testIsNotSet() throws Exception
    {
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1))) {
            client.start();

            final DistributedBarrier barrier = new DistributedBarrier(client, "/barrier");
            barrier.setBarrier();
            barrier.removeBarrier();

            assertFalse(barrier.isSet());
        }
    }

    @Test
    public void     testWatchersRemoved() throws Exception
    {
        CuratorFramework              client = mock(CuratorFramework.class);
        WatcherRemoveCuratorFramework watcherRemoveClient = mock(WatcherRemoveCuratorFramework.class);
        ExistsBuilder                 existsBuilder = mock(ExistsBuilder.class);

        when(client.newWatcherRemoveCuratorFramework()).thenReturn(watcherRemoveClient);
        when(watcherRemoveClient.checkExists()).thenReturn(existsBuilder);
        when(existsBuilder.usingWatcher(any(Watcher.class))).thenReturn(existsBuilder);

        final DistributedBarrier      barrier = new DistributedBarrier(client, "/barrier");
        barrier.waitOnBarrier(1, TimeUnit.SECONDS);
        verify(watcherRemoveClient, atLeastOnce()).removeWatchers();
    }

}
