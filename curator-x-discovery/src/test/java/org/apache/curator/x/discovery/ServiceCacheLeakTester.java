package org.apache.curator.x.discovery;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.strategies.RandomStrategy;
import org.testng.annotations.Test;

public class ServiceCacheLeakTester
{
    @Test
    public void serviceCacheInstancesLeaked() throws Exception
    {
        TestingServer testingServer = new TestingServer();

        final CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), new RetryOneTime(1));
        try
        {
            curatorFramework.start();

            doWork(curatorFramework);
            System.gc();

            System.out.println("Done - get dump");
            Thread.currentThread().join();
        }
        finally
        {
            CloseableUtils.closeQuietly(curatorFramework);
            CloseableUtils.closeQuietly(testingServer);
        }
    }

    private void doWork(CuratorFramework curatorFramework) throws Exception
    {
        ServiceInstance<Void> thisInstance = ServiceInstance.<Void>builder().name("myservice").build();
        final ServiceDiscovery<Void> serviceDiscovery = ServiceDiscoveryBuilder.builder(Void.class).client(curatorFramework.usingNamespace("dev")).basePath("/instances").thisInstance(thisInstance).build();
        serviceDiscovery.start();

        for ( int i = 0; i < 100000; i++ )
        {
            final ServiceProvider<Void> s = serviceProvider(serviceDiscovery, "myservice");
            s.start();
            try
            {
                s.getInstance().buildUriSpec();
            }
            finally
            {
                s.close();
            }
        }
    }

    private ServiceProvider<Void> serviceProvider(ServiceDiscovery<Void> serviceDiscovery, String name) throws Exception
    {
        return serviceDiscovery.serviceProviderBuilder().serviceName(name).providerStrategy(new RandomStrategy<Void>()).build();
    }
}