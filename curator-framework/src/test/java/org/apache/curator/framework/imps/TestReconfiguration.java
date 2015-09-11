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

import org.apache.curator.ensemble.EnsembleListener;
import org.apache.curator.ensemble.dynamic.DynamicEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.ensemble.EnsembleTracker;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class TestReconfiguration extends BaseClassForTests
{
    private static final Timing timing = new Timing();
    private TestingCluster cluster;
    private DynamicEnsembleProvider dynamicEnsembleProvider;
    private WaitOnDelegateListener waitOnDelegateListener;
    private EnsembleTracker ensembleTracker;
    private CuratorFramework client;

    private String connectionString1to5;
    private String connectionString2to5;
    private String connectionString3to5;

    @BeforeMethod
    public void setup() throws Exception
    {
        cluster = new TestingCluster(5);
        cluster.start();

        connectionString1to5 = cluster.getConnectString();
        connectionString2to5 = getConnectionString(cluster, 2, 3, 4, 5);
        connectionString3to5 = getConnectionString(cluster, 3, 4, 5);

        dynamicEnsembleProvider = new DynamicEnsembleProvider(connectionString1to5);
        client = CuratorFrameworkFactory.builder()
            .ensembleProvider(dynamicEnsembleProvider)
            .retryPolicy(new RetryOneTime(1))
            .build();
        client.start();
        client.blockUntilConnected();

        //Wrap around the dynamic ensemble provider, so that we can wait until it has received the event.
        waitOnDelegateListener = new WaitOnDelegateListener(dynamicEnsembleProvider);
        ensembleTracker = new EnsembleTracker(client);
        ensembleTracker.getListenable().addListener(waitOnDelegateListener);
        ensembleTracker.start();
        //Wait for the initial event.
        waitOnDelegateListener.waitForEvent();
    }

    @AfterMethod
    public void tearDown() throws IOException
    {
        CloseableUtils.closeQuietly(ensembleTracker);
        CloseableUtils.closeQuietly(client);
        CloseableUtils.closeQuietly(cluster);
    }

    @Test
    public void testSyncIncremental() throws Exception
    {
        Stat stat = new Stat();
        byte[] bytes = client.getConfig().storingStatIn(stat).forEnsemble();
        Assert.assertNotNull(bytes);
        QuorumVerifier qv = getQuorumVerifier(bytes);
        Assert.assertEquals(qv.getAllMembers().size(), 5);
        String server1 = getServerString(qv, cluster, 1L);
        String server2 = getServerString(qv, cluster, 2L);

        //Remove Servers
        bytes = client.reconfig().leaving("1").fromConfig(qv.getVersion()).storingStatIn(stat).forEnsemble();
        qv = getQuorumVerifier(bytes);
        Assert.assertEquals(qv.getAllMembers().size(), 4);

        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString2to5);

        bytes = client.reconfig().leaving("2").fromConfig(qv.getVersion()).storingStatIn(stat).forEnsemble();
        qv = getQuorumVerifier(bytes);
        Assert.assertEquals(qv.getAllMembers().size(), 3);

        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString3to5);

        //Add Servers
        bytes = client.reconfig().joining("server.2=" + server2).fromConfig(qv.getVersion()).storingStatIn(stat).forEnsemble();
        qv = getQuorumVerifier(bytes);
        Assert.assertEquals(qv.getAllMembers().size(), 4);

        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString2to5);

        bytes = client.reconfig().joining("server.1=" + server1).fromConfig(qv.getVersion()).storingStatIn(stat).forEnsemble();
        qv = getQuorumVerifier(bytes);
        Assert.assertEquals(qv.getAllMembers().size(), 5);

        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString1to5);
    }

    @Test
    public void testAsyncIncremental() throws Exception
    {
        final AtomicReference<byte[]> bytes = new AtomicReference<>();
        final BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                bytes.set(event.getData());
                //We only need the latch on getConfig.
                if ( event.getContext() != null )
                {
                    ((CountDownLatch)event.getContext()).countDown();
                }
            }

        };

        CountDownLatch latch = new CountDownLatch(1);
        client.getConfig().inBackground(callback, latch).forEnsemble();
        Assert.assertTrue(timing.awaitLatch(latch));
        Assert.assertNotNull(bytes.get());
        QuorumVerifier qv = getQuorumVerifier(bytes.get());
        Assert.assertEquals(qv.getAllMembers().size(), 5);
        String server1 = getServerString(qv, cluster, 1L);
        String server2 = getServerString(qv, cluster, 2L);

        //Remove Servers
        client.reconfig().inBackground(callback).leaving("1").fromConfig(qv.getVersion()).forEnsemble();
        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString2to5);
        qv = getQuorumVerifier(bytes.get());
        Assert.assertEquals(qv.getAllMembers().size(), 4);

        client.reconfig().inBackground(callback, latch).leaving("2").fromConfig(qv.getVersion()).forEnsemble();
        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString3to5);
        qv = getQuorumVerifier(bytes.get());
        Assert.assertEquals(qv.getAllMembers().size(), 3);

        //Add Servers
        client.reconfig().inBackground(callback, latch).joining("server.2=" + server2).fromConfig(qv.getVersion()).forEnsemble();
        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString2to5);
        qv = getQuorumVerifier(bytes.get());
        Assert.assertEquals(qv.getAllMembers().size(), 4);

        client.reconfig().inBackground(callback, latch).joining("server.1=" + server1).fromConfig(qv.getVersion()).forEnsemble();
        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString1to5);
        qv = getQuorumVerifier(bytes.get());
        Assert.assertEquals(qv.getAllMembers().size(), 5);
    }

    @Test
    public void testSyncNonIncremental() throws Exception
    {
        Stat stat = new Stat();
        byte[] bytes = client.getConfig().storingStatIn(stat).forEnsemble();
        Assert.assertNotNull(bytes);
        QuorumVerifier qv = getQuorumVerifier(bytes);
        Assert.assertEquals(qv.getAllMembers().size(), 5);
        String server1 = getServerString(qv, cluster, 1L);
        String server2 = getServerString(qv, cluster, 2L);
        String server3 = getServerString(qv, cluster, 3L);
        String server4 = getServerString(qv, cluster, 4L);
        String server5 = getServerString(qv, cluster, 5L);

        //Remove Servers
        bytes = client.reconfig()
            .adding("server.2=" + server2,
                "server.3=" + server3,
                "server.4=" + server4,
                "server.5=" + server5)
            .fromConfig(qv.getVersion()).storingStatIn(stat).forEnsemble();
        qv = getQuorumVerifier(bytes);
        Assert.assertEquals(qv.getAllMembers().size(), 4);

        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString2to5);

        bytes = client.reconfig()
            .adding("server.3=" + server3,
                "server.4=" + server4,
                "server.5=" + server5)
            .fromConfig(qv.getVersion()).storingStatIn(stat).forEnsemble();

        qv = getQuorumVerifier(bytes);
        Assert.assertEquals(qv.getAllMembers().size(), 3);

        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString3to5);

        //Add Servers
        bytes = client.reconfig()
            .adding("server.2=" + server2,
                "server.3=" + server3,
                "server.4=" + server4,
                "server.5=" + server5)
            .fromConfig(qv.getVersion()).storingStatIn(stat).forEnsemble();
        qv = getQuorumVerifier(bytes);
        Assert.assertEquals(qv.getAllMembers().size(), 4);

        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString2to5);

        bytes = client.reconfig()
            .adding("server.1=" + server1,
                "server.2=" + server2,
                "server.3=" + server3,
                "server.4=" + server4,
                "server.5=" + server5)
            .fromConfig(qv.getVersion()).storingStatIn(stat).forEnsemble();
        qv = getQuorumVerifier(bytes);
        Assert.assertEquals(qv.getAllMembers().size(), 5);

        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString1to5);
    }

    @Test
    public void testAsyncNonIncremental() throws Exception
    {
        final AtomicReference<byte[]> bytes = new AtomicReference<>();
        final BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                bytes.set(event.getData());
                ((CountDownLatch)event.getContext()).countDown();
            }

        };

        CountDownLatch latch = new CountDownLatch(1);
        client.getConfig().inBackground(callback, latch).forEnsemble();
        Assert.assertTrue(timing.awaitLatch(latch));
        Assert.assertNotNull(bytes.get());
        QuorumVerifier qv = getQuorumVerifier(bytes.get());
        Assert.assertEquals(qv.getAllMembers().size(), 5);
        String server1 = getServerString(qv, cluster, 1L);
        String server2 = getServerString(qv, cluster, 2L);
        String server3 = getServerString(qv, cluster, 3L);
        String server4 = getServerString(qv, cluster, 4L);
        String server5 = getServerString(qv, cluster, 5L);

        //Remove Servers
        client.reconfig().inBackground(callback, latch)
            .adding("server.2=" + server2,
                "server.3=" + server3,
                "server.4=" + server4,
                "server.5=" + server5)
            .fromConfig(qv.getVersion()).forEnsemble();
        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString2to5);
        qv = getQuorumVerifier(bytes.get());
        Assert.assertEquals(qv.getAllMembers().size(), 4);

        client.reconfig().inBackground(callback, latch)
            .adding("server.3=" + server3,
                "server.4=" + server4,
                "server.5=" + server5)
            .fromConfig(qv.getVersion()).forEnsemble();
        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString3to5);
        qv = getQuorumVerifier(bytes.get());
        Assert.assertEquals(qv.getAllMembers().size(), 3);

        //Add Servers
        client.reconfig().inBackground(callback, latch)
            .adding("server.2=" + server2,
                "server.3=" + server3,
                "server.4=" + server4,
                "server.5=" + server5)
            .fromConfig(qv.getVersion()).forEnsemble();
        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString2to5);
        qv = getQuorumVerifier(bytes.get());
        Assert.assertEquals(qv.getAllMembers().size(), 4);

        client.reconfig().inBackground(callback, latch)
            .adding("server.1=" + server1,
                "server.2=" + server2,
                "server.3=" + server3,
                "server.4=" + server4,
                "server.5=" + server5)
            .fromConfig(qv.getVersion()).forEnsemble();
        waitOnDelegateListener.waitForEvent();
        Assert.assertEquals(dynamicEnsembleProvider.getConnectionString(), connectionString1to5);
        qv = getQuorumVerifier(bytes.get());
        Assert.assertEquals(qv.getAllMembers().size(), 5);
    }

    static QuorumVerifier getQuorumVerifier(byte[] bytes) throws Exception
    {
        Properties properties = new Properties();
        properties.load(new StringReader(new String(bytes)));
        return new QuorumMaj(properties);
    }

    static InstanceSpec getInstance(TestingCluster cluster, int id)
    {
        for ( InstanceSpec spec : cluster.getInstances() )
        {
            if ( spec.getServerId() == id )
            {
                return spec;
            }
        }
        throw new IllegalStateException("InstanceSpec with id:" + id + " not found");
    }

    static String getServerString(QuorumVerifier qv, TestingCluster cluster, long id) throws Exception
    {
        String str = qv.getAllMembers().get(id).toString();
        //check if connection string is already there.
        if ( str.contains(";") )
        {
            return str;
        }
        else
        {
            return str + ";" + getInstance(cluster, (int)id).getConnectString();
        }
    }

    static String getConnectionString(TestingCluster cluster, long... ids) throws Exception
    {
        StringBuilder sb = new StringBuilder();
        Map<Long, InstanceSpec> specs = new HashMap<>();
        for ( InstanceSpec spec : cluster.getInstances() )
        {
            specs.put((long)spec.getServerId(), spec);
        }
        for ( long id : ids )
        {
            if ( sb.length() != 0 )
            {
                sb.append(",");
            }
            sb.append(specs.get(id).getConnectString());
        }
        return sb.toString();
    }

    //Simple EnsembleListener that can wait until the delegate handles the event.
    private static class WaitOnDelegateListener implements EnsembleListener
    {
        private CountDownLatch latch = new CountDownLatch(1);

        private final EnsembleListener delegate;

        private WaitOnDelegateListener(EnsembleListener delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void connectionStringUpdated(String connectionString)
        {
            delegate.connectionStringUpdated(connectionString);
            latch.countDown();
        }

        public void waitForEvent() throws InterruptedException, TimeoutException
        {
            if ( timing.awaitLatch(latch) )
            {
                latch = new CountDownLatch(1);
            }
            else
            {
                throw new TimeoutException("Failed to receive event in time.");
            }
        }
    }
}