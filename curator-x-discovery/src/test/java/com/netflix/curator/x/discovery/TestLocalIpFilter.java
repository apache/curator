package com.netflix.curator.x.discovery;

import com.google.common.collect.Lists;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.List;

public class TestLocalIpFilter
{
    @Test
    public void     testFilterEverything() throws SocketException
    {
        LocalIpFilter localIpFilter = ServiceInstanceBuilder.getLocalIpFilter();
        try
        {
            ServiceInstanceBuilder.setLocalIpFilter
                (
                    new LocalIpFilter()
                    {
                        @Override
                        public boolean use(NetworkInterface networkInterface, InetAddress address) throws SocketException
                        {
                            return false;
                        }
                    }
                );

            List<InetAddress> allLocalIPs = Lists.newArrayList(ServiceInstanceBuilder.getAllLocalIPs());
            Assert.assertEquals(allLocalIPs.size(), 0);
        }
        finally
        {
            ServiceInstanceBuilder.setLocalIpFilter(localIpFilter);
        }

        List<InetAddress> allLocalIPs = Lists.newArrayList(ServiceInstanceBuilder.getAllLocalIPs());
        Assert.assertTrue(allLocalIPs.size() > 0);
    }
}
