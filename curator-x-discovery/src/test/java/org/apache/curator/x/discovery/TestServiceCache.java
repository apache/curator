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

package org.apache.curator.x.discovery;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.ExecuteCalledWatchingExecutorService;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class TestServiceCache extends BaseClassForTests
{
    @Test
    public void testInitialLoad() throws Exception
    {
        List<Closeable> closeables = Lists.newArrayList();
        try
        {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceDiscovery<String> discovery = ServiceDiscoveryBuilder.builder(String.class).basePath("/discovery").client(client).build();
            closeables.add(discovery);
            discovery.start();

            ServiceCache<String> cache = discovery.serviceCacheBuilder().name("test").build();
            closeables.add(cache);

            final CountDownLatch latch = new CountDownLatch(3);
            ServiceCacheListener listener = new ServiceCacheListener()
            {
                @Override
                public void cacheChanged()
                {
                    latch.countDown();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };
            cache.addListener(listener);
            cache.start();

            ServiceInstance<String> instance1 = ServiceInstance.<String>builder().payload("test").name("test").port(10064).build();
            ServiceInstance<String> instance2 = ServiceInstance.<String>builder().payload("test").name("test").port(10065).build();
            ServiceInstance<String> instance3 = ServiceInstance.<String>builder().payload("test").name("test").port(10066).build();
            discovery.registerService(instance1);
            discovery.registerService(instance2);
            discovery.registerService(instance3);

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

            ServiceCache<String> cache2 = discovery.serviceCacheBuilder().name("test").build();
            closeables.add(cache2);
            cache2.start();

            Assert.assertEquals(cache2.getInstances().size(), 3);
        }
        finally
        {
            Collections.reverse(closeables);
            for ( Closeable c : closeables )
            {
                CloseableUtils.closeQuietly(c);
            }
        }
    }

    @Test
    public void testViaProvider() throws Exception
    {
        Timing timing = new Timing();

        List<Closeable> closeables = Lists.newArrayList();
        try
        {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceDiscovery<String> discovery = ServiceDiscoveryBuilder.builder(String.class).basePath("/discovery").client(client).build();
            closeables.add(discovery);
            discovery.start();

            ServiceProvider<String> serviceProvider = discovery.serviceProviderBuilder().serviceName("test").build();
            closeables.add(serviceProvider);
            serviceProvider.start();

            ServiceInstance<String> instance = ServiceInstance.<String>builder().payload("thing").name("test").port(10064).build();
            discovery.registerService(instance);

            int count = 0;
            ServiceInstance<String> foundInstance = null;
            while ( foundInstance == null )
            {
                Assert.assertTrue(count++ < 5);
                foundInstance = serviceProvider.getInstance();
                timing.sleepABit();
            }
            Assert.assertEquals(foundInstance, instance);

            ServiceInstance<String> instance2 = ServiceInstance.<String>builder().address("foo").payload("thing").name("test").port(10064).build();
            discovery.registerService(instance2);
            timing.sleepABit();
            Collection<ServiceInstance<String>> allInstances = serviceProvider.getAllInstances();
            Assert.assertEquals(allInstances.size(), 2);
        }
        finally
        {
            Collections.reverse(closeables);
            for ( Closeable c : closeables )
            {
                CloseableUtils.closeQuietly(c);
            }
        }
    }

    @Test
    public void testUpdate() throws Exception
    {
        List<Closeable> closeables = Lists.newArrayList();
        try
        {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceInstance<String> instance = ServiceInstance.<String>builder().payload("thing").name("test").port(10064).build();
            ServiceDiscovery<String> discovery = ServiceDiscoveryBuilder.builder(String.class).basePath("/test").client(client).thisInstance(instance).build();
            closeables.add(discovery);
            discovery.start();

            final CountDownLatch latch = new CountDownLatch(1);
            ServiceCache<String> cache = discovery.serviceCacheBuilder().name("test").build();
            closeables.add(cache);
            ServiceCacheListener listener = new ServiceCacheListener()
            {
                @Override
                public void cacheChanged()
                {
                    latch.countDown();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                }
            };
            cache.addListener(listener);
            cache.start();

            instance = ServiceInstance.<String>builder().payload("changed").name("test").port(10064).id(instance.getId()).build();
            discovery.updateService(instance);

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

            Assert.assertEquals(cache.getInstances().size(), 1);
            Assert.assertEquals(cache.getInstances().get(0).getPayload(), instance.getPayload());
        }
        finally
        {
            Collections.reverse(closeables);
            for ( Closeable c : closeables )
            {
                CloseableUtils.closeQuietly(c);
            }
        }
    }

    @Test
    public void testCache() throws Exception
    {
        List<Closeable> closeables = Lists.newArrayList();
        try
        {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceDiscovery<String> discovery = ServiceDiscoveryBuilder.builder(String.class).basePath("/discovery").client(client).build();
            closeables.add(discovery);
            discovery.start();

            ServiceCache<String> cache = discovery.serviceCacheBuilder().name("test").build();
            closeables.add(cache);
            cache.start();

            final Semaphore semaphore = new Semaphore(0);
            ServiceCacheListener listener = new ServiceCacheListener()
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

            ServiceInstance<String> instance1 = ServiceInstance.<String>builder().payload("thing").name("test").port(10064).build();
            ServiceInstance<String> instance2 = ServiceInstance.<String>builder().payload("thing").name("test").port(10065).build();
            discovery.registerService(instance1);
            Assert.assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));

            discovery.registerService(instance2);
            Assert.assertTrue(semaphore.tryAcquire(3, TimeUnit.SECONDS));

            ServiceInstance<String> instance3 = ServiceInstance.<String>builder().payload("thing").name("another").port(10064).build();
            discovery.registerService(instance3);
            Assert.assertFalse(semaphore.tryAcquire(3, TimeUnit.SECONDS));  // should not get called for a different service
        }
        finally
        {
            Collections.reverse(closeables);
            for ( Closeable c : closeables )
            {
                CloseableUtils.closeQuietly(c);
            }
        }
    }

    @Test
    public void testExecutorServiceIsInvoked() throws Exception
    {
        List<Closeable> closeables = Lists.newArrayList();
        try
        {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceDiscovery<String> discovery = ServiceDiscoveryBuilder.builder(String.class).basePath("/discovery").client(client).build();
            closeables.add(discovery);
            discovery.start();

            ExecuteCalledWatchingExecutorService exec = new ExecuteCalledWatchingExecutorService(Executors.newSingleThreadExecutor());
            Assert.assertFalse(exec.isExecuteCalled());

            ServiceCache<String> cache = discovery.serviceCacheBuilder().name("test").executorService(exec).build();
            closeables.add(cache);
            cache.start();

            final Semaphore semaphore = new Semaphore(0);
            ServiceCacheListener listener = new ServiceCacheListener()
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

            ServiceInstance<String> instance1 = ServiceInstance.<String>builder().payload("thing").name("test").port(10064).build();
            discovery.registerService(instance1);
            Assert.assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));

            Assert.assertTrue(exec.isExecuteCalled());
        }
        finally
        {
            Collections.reverse(closeables);
            for ( Closeable c : closeables )
            {
                CloseableUtils.closeQuietly(c);
            }
        }
    }
}
