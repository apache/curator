package org.apache.curator.x.discovery.details;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

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
            Assert.assertTrue(timing.awaitLatch(cacheStartLatch));  // wait until ServiceCacheImpl's internal PathChildrenCache is started and primed

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
            Assert.assertTrue(timing.awaitLatch(cacheChangedLatch));

            cacheWaitLatch.countDown();

            Assert.assertTrue(timing.awaitLatch(startCompletedLatch));
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
