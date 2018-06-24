package org.apache.curator.x.discovery.details;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by wei.w on 4/2/18.
 */
public class TestServiceCacheImpl extends BaseClassForTests {

    public void testInitialLoadRaceCondition(boolean oldApi) throws Exception {

        List<Closeable> closeables = Lists.newArrayList();
        try {
            CuratorFramework initClient = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(initClient);
            initClient.start();

            ServiceDiscovery<String> initDiscovery = ServiceDiscoveryBuilder.builder(String.class).basePath("/discovery").client(initClient).build();
            closeables.add(initDiscovery);
            initDiscovery.start();

            ServiceInstance<String> instance1 = ServiceInstance.<String>builder().payload("test").name("test").port(10064).build();
            ServiceInstance<String> instance2 = ServiceInstance.<String>builder().payload("test").name("test").port(10065).build();
            ServiceInstance<String> instance3 = ServiceInstance.<String>builder().payload("test").name("test").port(10066).build();
            initDiscovery.registerService(instance1);
            initDiscovery.registerService(instance2);
            initDiscovery.registerService(instance3);


            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            closeables.add(client);

            ServiceDiscovery<String> discovery = ServiceDiscoveryBuilder.builder(String.class).basePath("/discovery").client(client).build();
            closeables.add(discovery);
            discovery.start();

            ServiceCacheImpl cache = (ServiceCacheImpl) discovery.serviceCacheBuilder().name("test").build();
            closeables.add(cache);

            final CountDownLatch latch = new CountDownLatch(3);

            ServiceCacheListener listener = new ServiceCacheListener() {
                @Override
                public void cacheChanged() {
                    latch.countDown();

                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                    }
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                }
            };
            cache.addListener(listener);
            client.start();

            cache.startCache();
            Assert.assertTrue(latch.await(300, TimeUnit.SECONDS));
            if (oldApi) {
                cache.oldLloadCacheData();
            } else {
                cache.loadCacheData();
            }
        } finally {
            Collections.reverse(closeables);
            for (Closeable c : closeables) {
                CloseableUtils.closeQuietly(c);
            }
        }
    }


    @Test(expectedExceptions = NullPointerException.class)
    public void testInitialLoadRaceConditionOldApi() throws Exception {
        testInitialLoadRaceCondition(true);

    }

    @Test
    public void testInitialLoadRaceConditionNewApi() throws Exception {
        testInitialLoadRaceCondition(false);
    }

}
