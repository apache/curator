/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.x.discovery;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.utils.TestingServer;
import junit.framework.Assert;
import org.testng.annotations.Test;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class TestServiceCache
{
    @Test
    public void testCache() throws Exception
    {
        List<Closeable> closeables = Lists.newArrayList();
        TestingServer server = new TestingServer();
        closeables.add(server);
        try
        {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceDiscovery<String>    discovery = new ServiceDiscovery<String>(client, "/discovery", new JsonInstanceSerializer<String>(String.class));
            closeables.add(discovery);
            discovery.start();

            ServiceCache<String> cache = discovery.serviceCacheBuilder().name("test").build();
            closeables.add(cache);
            cache.start();

            final Semaphore semaphore = new Semaphore(0);
            ServiceCacheListener    listener = new ServiceCacheListener()
            {
                @Override
                public void cacheChanged()
                {
                    semaphore.release();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };
            cache.addListener(listener);

            ServiceInstance<String>     instance = ServiceInstance.<String>builder().payload("thing").name("test").port(10064).build();
            discovery.registerService(instance);
            Assert.assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));

            discovery.registerService(instance);
            Assert.assertFalse(semaphore.tryAcquire(3, TimeUnit.SECONDS));  // should not get called for a re-registration

            instance = ServiceInstance.<String>builder().payload("thing").name("another").port(10064).build();
            discovery.registerService(instance);
            Assert.assertFalse(semaphore.tryAcquire(3, TimeUnit.SECONDS));  // should not get called for a different service
        }
        finally
        {
            Collections.reverse(closeables);
            for ( Closeable c : closeables )
            {
                Closeables.closeQuietly(c);
            }
        }
    }
}
