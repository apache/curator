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

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TestReconfiguration extends BaseClassForTests
{
    private TestingCluster cluster;

    @BeforeMethod
    @Override
    public void setup() throws Exception
    {
        super.setup();

        CloseableUtils.closeQuietly(server);
        server = null;
        cluster = new TestingCluster(3);
        cluster.start();
    }

    @AfterMethod
    @Override
    public void teardown() throws Exception
    {
        CloseableUtils.closeQuietly(cluster);

        super.teardown();
    }

    @SuppressWarnings("ConstantConditions")
    public void testApiPermutations() throws Exception
    {
        // not an actual test. Specifies all possible API possibilities

        Watcher watcher = null;
        Stat stat = null;
        CuratorFramework client = null;

        client.getConfig().forEnsemble();
        client.getConfig().inBackground().forEnsemble();
        client.getConfig().usingWatcher(watcher).forEnsemble();
        client.getConfig().usingWatcher(watcher).inBackground().forEnsemble();
        client.getConfig().storingStatIn(stat).forEnsemble();
        client.getConfig().storingStatIn(stat).inBackground().forEnsemble();
        client.getConfig().storingStatIn(stat).usingWatcher(watcher).forEnsemble();
        client.getConfig().storingStatIn(stat).usingWatcher(watcher).inBackground().forEnsemble();

        // ---------

        client.reconfig().leaving().forEnsemble();
        client.reconfig().joining().forEnsemble();
        client.reconfig().leaving().joining().forEnsemble();
        client.reconfig().joining().leaving().forEnsemble();
        client.reconfig().withNewMembers().forEnsemble();

        client.reconfig().leaving().fromConfig(0).forEnsemble();
        client.reconfig().joining().fromConfig(0).forEnsemble();
        client.reconfig().leaving().joining().fromConfig(0).forEnsemble();
        client.reconfig().joining().leaving().fromConfig(0).forEnsemble();
        client.reconfig().withNewMembers().fromConfig(0).forEnsemble();

        client.reconfig().leaving().storingStatIn(stat).forEnsemble();
        client.reconfig().joining().storingStatIn(stat).forEnsemble();
        client.reconfig().leaving().joining().storingStatIn(stat).forEnsemble();
        client.reconfig().joining().leaving().storingStatIn(stat).forEnsemble();
        client.reconfig().withNewMembers().storingStatIn(stat).forEnsemble();

        client.reconfig().leaving().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().joining().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().leaving().joining().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().joining().leaving().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().withNewMembers().storingStatIn(stat).forEnsemble();

        client.reconfig().inBackground().leaving().forEnsemble();
        client.reconfig().inBackground().joining().forEnsemble();
        client.reconfig().inBackground().leaving().joining().forEnsemble();
        client.reconfig().inBackground().joining().leaving().forEnsemble();
        client.reconfig().inBackground().withNewMembers().forEnsemble();

        client.reconfig().inBackground().leaving().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().joining().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().leaving().joining().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().joining().leaving().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().withNewMembers().fromConfig(0).forEnsemble();

        client.reconfig().inBackground().leaving().storingStatIn(stat).forEnsemble();
        client.reconfig().inBackground().joining().storingStatIn(stat).forEnsemble();
        client.reconfig().inBackground().leaving().joining().storingStatIn(stat).forEnsemble();
        client.reconfig().inBackground().joining().leaving().storingStatIn(stat).forEnsemble();
        client.reconfig().inBackground().withNewMembers().storingStatIn(stat).forEnsemble();

        client.reconfig().inBackground().leaving().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().inBackground().joining().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().inBackground().leaving().joining().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().inBackground().joining().leaving().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().inBackground().withNewMembers().storingStatIn(stat).forEnsemble();
    }

    @Test
    public void testBasicGetConfig() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();
            QuorumVerifier quorumVerifier = toQuorumVerifier(client.getConfig().forEnsemble());
            System.out.println(quorumVerifier);
            assertConfig(quorumVerifier, cluster.getInstances());
        }
    }

    @Test
    public void testAdd() throws Exception
    {
        Timing timing = new Timing();
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            QuorumVerifier oldConfig = toQuorumVerifier(client.getConfig().forEnsemble());
            assertConfig(oldConfig, cluster.getInstances());

            CountDownLatch latch = setChangeWaiter(client);
            try ( TestingCluster newCluster = new TestingCluster(1, false) )
            {
                newCluster.start();

                client.reconfig().joining(toReconfigSpec(newCluster.getInstances())).fromConfig(oldConfig.getVersion()).forEnsemble();

                Assert.assertTrue(timing.awaitLatch(latch));

                QuorumVerifier newConfig = toQuorumVerifier(client.getConfig().forEnsemble());
                List<InstanceSpec> newInstances = Lists.newArrayList(cluster.getInstances());
                newInstances.addAll(newCluster.getInstances());
                assertConfig(newConfig, newInstances);
            }
        }
    }

    @Test
    public void testAddAsync() throws Exception
    {
        Timing timing = new Timing();
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            QuorumVerifier oldConfig = toQuorumVerifier(client.getConfig().forEnsemble());
            assertConfig(oldConfig, cluster.getInstances());

            CountDownLatch latch = setChangeWaiter(client);
            try ( TestingCluster newCluster = new TestingCluster(1, false) )
            {
                newCluster.start();

                final CountDownLatch callbackLatch = new CountDownLatch(1);
                BackgroundCallback callback = new BackgroundCallback()
                {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                    {
                        if ( event.getType() == CuratorEventType.RECONFIG )
                        {
                            callbackLatch.countDown();
                        }
                    }
                };
                client.reconfig().inBackground(callback).joining(toReconfigSpec(newCluster.getInstances())).fromConfig(oldConfig.getVersion()).forEnsemble();

                Assert.assertTrue(timing.awaitLatch(callbackLatch));
                Assert.assertTrue(timing.awaitLatch(latch));

                QuorumVerifier newConfig = toQuorumVerifier(client.getConfig().forEnsemble());
                List<InstanceSpec> newInstances = Lists.newArrayList(cluster.getInstances());
                newInstances.addAll(newCluster.getInstances());
                assertConfig(newConfig, newInstances);
            }
        }
    }

    @Test
    public void testAddAndRemove() throws Exception
    {
        Timing timing = new Timing();
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            QuorumVerifier oldConfig = toQuorumVerifier(client.getConfig().forEnsemble());
            assertConfig(oldConfig, cluster.getInstances());

            CountDownLatch latch = setChangeWaiter(client);

            try ( TestingCluster newCluster = new TestingCluster(1, false) )
            {
                newCluster.start();

                Collection<InstanceSpec> oldInstances = cluster.getInstances();
                InstanceSpec us = cluster.findConnectionInstance(client.getZookeeperClient().getZooKeeper());
                InstanceSpec removeSpec = oldInstances.iterator().next();
                if ( us.equals(removeSpec) ) {
                    Iterator<InstanceSpec> iterator = oldInstances.iterator();
                    iterator.next();
                    removeSpec = iterator.next();
                }

                Collection<InstanceSpec> instances = newCluster.getInstances();
                client.reconfig().leaving(Integer.toString(removeSpec.getServerId())).joining(toReconfigSpec(instances)).fromConfig(oldConfig.getVersion()).forEnsemble();

                Assert.assertTrue(timing.awaitLatch(latch));

                QuorumVerifier newConfig = toQuorumVerifier(client.getConfig().forEnsemble());
                ArrayList<InstanceSpec> newInstances = Lists.newArrayList(oldInstances);
                newInstances.addAll(instances);
                newInstances.remove(removeSpec);
                assertConfig(newConfig, newInstances);
            }
        }
    }

    private CountDownLatch setChangeWaiter(CuratorFramework client) throws Exception
    {
        final CountDownLatch latch = new CountDownLatch(1);
        Watcher watcher = new Watcher()
        {
            @Override
            public void process(WatchedEvent event)
            {
                if ( event.getType() == Event.EventType.NodeDataChanged )
                {
                    latch.countDown();
                }
            }
        };
        client.getConfig().usingWatcher(watcher).forEnsemble();
        return latch;
    }

    private void assertConfig(QuorumVerifier config, Collection<InstanceSpec> instances)
    {
        for ( InstanceSpec instance : instances )
        {
            QuorumPeer.QuorumServer quorumServer = config.getAllMembers().get((long)instance.getServerId());
            Assert.assertNotNull(quorumServer, String.format("Looking for %s - found %s", instance.getServerId(), config.getAllMembers()));
            Assert.assertEquals(quorumServer.clientAddr.getPort(), instance.getPort());
        }
    }

    private List<String> toReconfigSpec(Collection<InstanceSpec> instances)
    {
        List<String> specs = Lists.newArrayList();
        for ( InstanceSpec instance : instances ) {
            specs.add("server." + instance.getServerId() + "=localhost:" + instance.getElectionPort() + ":" + instance.getQuorumPort() + ";" + instance.getPort());
        }
        return specs;
    }

    private static QuorumVerifier toQuorumVerifier(byte[] bytes) throws Exception
    {
        Assert.assertNotNull(bytes);
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(bytes));
        return new QuorumMaj(properties);
    }
}