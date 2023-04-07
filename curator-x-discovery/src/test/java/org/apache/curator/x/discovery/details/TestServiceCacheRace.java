/*
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

package org.apache.curator.x.discovery.details;

import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

@Tag(CuratorTestBase.zk35TestCompatibilityGroup)
public class TestServiceCacheRace extends BaseClassForTests
{
    private final Timing timing = new Timing();

    // validates CURATOR-452 which exposed a race in ServiceCacheImpl's start() method caused by an optimization whereby it clears the dataBytes of its internal PathChildrenCache
    @Test
    public void testRaceOnInitialLoad() throws Exception
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

            CountDownLatch cacheStartLatch = new CountDownLatch(1);
            CountDownLatch cacheWaitLatch = new CountDownLatch(1);
            final ServiceCache<String> cache = discovery.serviceCacheBuilder().name("test").build();
            closeables.add(cache);
            ((ServiceCacheImpl)cache).debugStartLatch = cacheStartLatch;    // causes ServiceCacheImpl.start to notify just after starting its internal PathChildrenCache
            ((ServiceCacheImpl)cache).debugStartWaitLatch = cacheWaitLatch; // causes ServiceCacheImpl.start to wait before iterating over its internal PathChildrenCache

            ServiceInstance<String> instance1 = ServiceInstance.<String>builder().payload("test").name("test").port(10064).build();
            discovery.registerService(instance1);

            CloseableExecutorService closeableExecutorService = new CloseableExecutorService(Executors.newSingleThreadExecutor());
            closeables.add(closeableExecutorService);
            final CountDownLatch startCompletedLatch = new CountDownLatch(1);
            Runnable proc = new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        cache.start();
                        startCompletedLatch.countDown();
                    }
                    catch ( Exception e )
                    {
                        LoggerFactory.getLogger(getClass()).error("Start failed", e);
                        throw new RuntimeException(e);
                    }
                }
            };
            closeableExecutorService.submit(proc);
            assertTrue(timing.awaitLatch(cacheStartLatch));  // wait until ServiceCacheImpl's internal PathChildrenCache is started and primed

            final CountDownLatch cacheChangedLatch = new CountDownLatch(1);
            ServiceCacheListener listener = new ServiceCacheListener()
            {
                @Override
                public void cacheChanged()
                {
                    cacheChangedLatch.countDown();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    // NOP
                }
            };
            cache.addListener(listener);
            ServiceInstance<String> instance2 = ServiceInstance.<String>builder().payload("test").name("test").port(10065).build();
            discovery.registerService(instance2);   // cause ServiceCacheImpl's internal PathChildrenCache listener to get called which will clear the dataBytes
            assertTrue(timing.awaitLatch(cacheChangedLatch));

            cacheWaitLatch.countDown();

            assertTrue(timing.awaitLatch(startCompletedLatch));
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
