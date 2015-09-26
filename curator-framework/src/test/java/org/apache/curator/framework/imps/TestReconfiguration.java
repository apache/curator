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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
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
import java.util.Properties;

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

        client.reconfig().adding().forEnsemble();
        client.reconfig().leaving().forEnsemble();
        client.reconfig().joining().forEnsemble();
        client.reconfig().adding().leaving().forEnsemble();
        client.reconfig().adding().joining().forEnsemble();
        client.reconfig().leaving().joining().forEnsemble();

        client.reconfig().adding().fromConfig(0).forEnsemble();
        client.reconfig().leaving().fromConfig(0).forEnsemble();
        client.reconfig().joining().fromConfig(0).forEnsemble();
        client.reconfig().adding().leaving().fromConfig(0).forEnsemble();
        client.reconfig().adding().joining().fromConfig(0).forEnsemble();
        client.reconfig().leaving().joining().fromConfig(0).forEnsemble();

        client.reconfig().adding().fromConfig(0).forEnsemble();
        client.reconfig().leaving().fromConfig(0).forEnsemble();
        client.reconfig().joining().fromConfig(0).forEnsemble();
        client.reconfig().adding().leaving().fromConfig(0).forEnsemble();
        client.reconfig().adding().joining().fromConfig(0).forEnsemble();
        client.reconfig().leaving().joining().fromConfig(0).forEnsemble();

        client.reconfig().adding().storingStatIn(stat).forEnsemble();
        client.reconfig().leaving().storingStatIn(stat).forEnsemble();
        client.reconfig().joining().storingStatIn(stat).forEnsemble();
        client.reconfig().adding().leaving().storingStatIn(stat).forEnsemble();
        client.reconfig().adding().joining().storingStatIn(stat).forEnsemble();
        client.reconfig().leaving().joining().storingStatIn(stat).forEnsemble();

        client.reconfig().adding().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().leaving().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().joining().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().adding().leaving().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().adding().joining().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().leaving().joining().storingStatIn(stat).fromConfig(0).forEnsemble();

        client.reconfig().inBackground().adding().forEnsemble();
        client.reconfig().inBackground().leaving().forEnsemble();
        client.reconfig().inBackground().joining().forEnsemble();
        client.reconfig().inBackground().adding().leaving().forEnsemble();
        client.reconfig().inBackground().adding().joining().forEnsemble();
        client.reconfig().inBackground().leaving().joining().forEnsemble();

        client.reconfig().inBackground().adding().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().leaving().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().joining().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().adding().leaving().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().adding().joining().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().leaving().joining().fromConfig(0).forEnsemble();

        client.reconfig().inBackground().adding().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().leaving().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().joining().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().adding().leaving().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().adding().joining().fromConfig(0).forEnsemble();
        client.reconfig().inBackground().leaving().joining().fromConfig(0).forEnsemble();

        client.reconfig().inBackground().adding().storingStatIn(stat).forEnsemble();
        client.reconfig().inBackground().leaving().storingStatIn(stat).forEnsemble();
        client.reconfig().inBackground().joining().storingStatIn(stat).forEnsemble();
        client.reconfig().inBackground().adding().leaving().storingStatIn(stat).forEnsemble();
        client.reconfig().inBackground().adding().joining().storingStatIn(stat).forEnsemble();
        client.reconfig().inBackground().leaving().joining().storingStatIn(stat).forEnsemble();

        client.reconfig().inBackground().adding().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().inBackground().leaving().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().inBackground().joining().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().inBackground().adding().leaving().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().inBackground().adding().joining().storingStatIn(stat).fromConfig(0).forEnsemble();
        client.reconfig().inBackground().leaving().joining().storingStatIn(stat).fromConfig(0).forEnsemble();
    }

    @Test
    public void testBasicGetConfig() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();
            QuorumVerifier quorumVerifier = toQuorumVerifier(client.getConfig().forEnsemble());
            System.out.println(quorumVerifier);

            for ( InstanceSpec instance : cluster.getInstances() )
            {
                QuorumPeer.QuorumServer quorumServer = quorumVerifier.getAllMembers().get((long)instance.getServerId());
                Assert.assertNotNull(quorumServer);
                Assert.assertEquals(quorumServer.clientAddr.getPort(), instance.getPort());
            }
        }
    }

    @Test
    public void testAdd1Sync() throws Exception
    {
        try ( CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryOneTime(1)) )
        {
            client.start();

            Watcher watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {

                }
            };
            client.getConfig().usingWatcher(watcher).forEnsemble();
        }
    }

    private static QuorumVerifier toQuorumVerifier(byte[] bytes) throws Exception
    {
        Assert.assertNotNull(bytes);
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(bytes));
        return new QuorumMaj(properties);
    }
}