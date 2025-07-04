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

package org.apache.curator.framework.imps;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;
import org.apache.curator.test.compatibility.CuratorTestBase;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestReconfiguration extends CuratorTestBase {
    private final Timing2 timing = new Timing2();
    private TestingCluster cluster;
    private EnsembleProvider ensembleProvider;

    private static final String superUserPasswordDigest = "curator-test:zghsj3JfJqK7DbWf0RQ1BgbJH9w="; // ran from
    // DigestAuthenticationProvider.generateDigest(superUserPassword);
    private static final String superUserPassword = "curator-test";

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();

        QuorumPeerConfig.setReconfigEnabled(true);
        System.setProperty("zookeeper.DigestAuthenticationProvider.superDigest", superUserPasswordDigest);

        CloseableUtils.closeQuietly(server);
        cluster = createAndStartCluster(3);
    }

    @AfterEach
    @Override
    public void teardown() throws Exception {
        CloseableUtils.closeQuietly(cluster);
        ensembleProvider = null;
        System.clearProperty("zookeeper.DigestAuthenticationProvider.superDigest");

        super.teardown();
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    @Disabled
    public void testApiPermutations() throws Exception {
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
        client.getConfig()
                .storingStatIn(stat)
                .usingWatcher(watcher)
                .inBackground()
                .forEnsemble();

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
        client.reconfig().withNewMembers().storingStatIn(stat).fromConfig(0).forEnsemble();

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

        client.reconfig()
                .inBackground()
                .leaving()
                .storingStatIn(stat)
                .fromConfig(0)
                .forEnsemble();
        client.reconfig()
                .inBackground()
                .joining()
                .storingStatIn(stat)
                .fromConfig(0)
                .forEnsemble();
        client.reconfig()
                .inBackground()
                .leaving()
                .joining()
                .storingStatIn(stat)
                .fromConfig(0)
                .forEnsemble();
        client.reconfig()
                .inBackground()
                .joining()
                .leaving()
                .storingStatIn(stat)
                .fromConfig(0)
                .forEnsemble();
        client.reconfig()
                .inBackground()
                .withNewMembers()
                .storingStatIn(stat)
                .fromConfig(0)
                .forEnsemble();
    }

    @Test
    public void testBasicGetConfig() throws Exception {
        try (CuratorFramework client = newClient()) {
            client.start();
            byte[] configData = client.getConfig().forEnsemble();
            QuorumVerifier quorumVerifier = toQuorumVerifier(configData);
            System.out.println(quorumVerifier);
            assertConfig(quorumVerifier, cluster.getInstances());
            assertEquals(
                    EnsembleTracker.configToConnectionString(quorumVerifier), ensembleProvider.getConnectionString());
        }
    }

    @Test
    public void testAddWithoutEnsembleTracker() throws Exception {
        final String initialClusterCS = cluster.getConnectString();
        try (CuratorFramework client = newClient(cluster.getConnectString(), false)) {
            assertEquals(((CuratorFrameworkBase) client).getEnsembleTracker(), null);
            client.start();

            QuorumVerifier oldConfig = toQuorumVerifier(client.getConfig().forEnsemble());
            assertConfig(oldConfig, cluster.getInstances());

            CountDownLatch latch = setChangeWaiter(client);
            try (TestingCluster newCluster = new TestingCluster(TestingCluster.makeSpecs(1, false))) {
                newCluster.start();

                client.reconfig()
                        .joining(toReconfigSpec(newCluster.getInstances()))
                        .fromConfig(oldConfig.getVersion())
                        .forEnsemble();

                assertTrue(timing.awaitLatch(latch));

                byte[] newConfigData = client.getConfig().forEnsemble();
                QuorumVerifier newConfig = toQuorumVerifier(newConfigData);
                List<InstanceSpec> newInstances = Lists.newArrayList(cluster.getInstances());
                newInstances.addAll(newCluster.getInstances());
                assertConfig(newConfig, newInstances);
                assertEquals(ensembleProvider.getConnectionString(), initialClusterCS);
                assertNotEquals(
                        EnsembleTracker.configToConnectionString(newConfig), ensembleProvider.getConnectionString());
                assertEquals(client.getZookeeperClient().getCurrentConnectionString(), initialClusterCS);
                final CountDownLatch reconnectLatch = new CountDownLatch(1);
                client.getConnectionStateListenable().addListener((cfClient, newState) -> {
                    if (newState == ConnectionState.RECONNECTED) reconnectLatch.countDown();
                });
                client.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();
                assertTrue(reconnectLatch.await(2, TimeUnit.SECONDS));
                assertEquals(client.getZookeeperClient().getCurrentConnectionString(), initialClusterCS);
                assertEquals(ensembleProvider.getConnectionString(), initialClusterCS);
                newConfigData = client.getConfig().forEnsemble();
                assertNotEquals(
                        EnsembleTracker.configToConnectionString(newConfig), ensembleProvider.getConnectionString());
            }
        }
    }

    @Test
    public void testAdd() throws Exception {
        CountDownLatch ensembleLatch = new CountDownLatch(1);
        try (CuratorFramework client = newClient(cluster.getConnectString(), ensembleLatch)) {
            client.start();

            QuorumVerifier oldConfig = toQuorumVerifier(client.getConfig().forEnsemble());
            assertConfig(oldConfig, cluster.getInstances());

            CountDownLatch eventLatch = setChangeWaiter(client);
            try (TestingCluster newCluster = new TestingCluster(TestingCluster.makeSpecs(1, false))) {
                newCluster.start();

                client.reconfig()
                        .joining(toReconfigSpec(newCluster.getInstances()))
                        .fromConfig(oldConfig.getVersion())
                        .forEnsemble();

                assertTrue(timing.awaitLatch(eventLatch));
                assertTrue(timing.awaitLatch(ensembleLatch));

                byte[] newConfigData = client.getConfig().forEnsemble();
                QuorumVerifier newConfig = toQuorumVerifier(newConfigData);
                List<InstanceSpec> newInstances = Lists.newArrayList(cluster.getInstances());
                newInstances.addAll(newCluster.getInstances());
                assertConfig(newConfig, newInstances);
                assertEquals(
                        EnsembleTracker.configToConnectionString(newConfig), ensembleProvider.getConnectionString());
            }
        }
    }

    @Test
    public void testAddAsync() throws Exception {
        CountDownLatch ensembleLatch = new CountDownLatch(1);
        try (CuratorFramework client = newClient(cluster.getConnectString(), ensembleLatch)) {
            client.start();

            QuorumVerifier oldConfig = toQuorumVerifier(client.getConfig().forEnsemble());
            assertConfig(oldConfig, cluster.getInstances());

            CountDownLatch eventLatch = setChangeWaiter(client);
            try (TestingCluster newCluster = new TestingCluster(TestingCluster.makeSpecs(1, false))) {
                newCluster.start();

                final CountDownLatch callbackLatch = new CountDownLatch(1);
                BackgroundCallback callback = new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        if (event.getType() == CuratorEventType.RECONFIG) {
                            callbackLatch.countDown();
                        }
                    }
                };
                client.reconfig()
                        .inBackground(callback)
                        .joining(toReconfigSpec(newCluster.getInstances()))
                        .fromConfig(oldConfig.getVersion())
                        .forEnsemble();

                assertTrue(timing.awaitLatch(callbackLatch));
                assertTrue(timing.awaitLatch(eventLatch));
                assertTrue(timing.awaitLatch(ensembleLatch));

                byte[] newConfigData = client.getConfig().forEnsemble();
                QuorumVerifier newConfig = toQuorumVerifier(newConfigData);
                List<InstanceSpec> newInstances = Lists.newArrayList(cluster.getInstances());
                newInstances.addAll(newCluster.getInstances());
                assertConfig(newConfig, newInstances);
                assertEquals(
                        EnsembleTracker.configToConnectionString(newConfig), ensembleProvider.getConnectionString());
            }
        }
    }

    @Test
    public void testAddAndRemove() throws Exception {
        CountDownLatch ensembleLatch = new CountDownLatch(1);
        try (CuratorFramework client = newClient(cluster.getConnectString(), ensembleLatch)) {
            client.start();

            QuorumVerifier oldConfig = toQuorumVerifier(client.getConfig().forEnsemble());
            assertConfig(oldConfig, cluster.getInstances());

            CountDownLatch eventLatch = setChangeWaiter(client);

            try (TestingCluster newCluster = new TestingCluster(TestingCluster.makeSpecs(1, false))) {
                newCluster.start();

                Collection<InstanceSpec> oldInstances = cluster.getInstances();
                InstanceSpec us = cluster.findConnectionInstance(
                        client.getZookeeperClient().getZooKeeper());
                InstanceSpec removeSpec = oldInstances.iterator().next();
                if (us.equals(removeSpec)) {
                    Iterator<InstanceSpec> iterator = oldInstances.iterator();
                    iterator.next();
                    removeSpec = iterator.next();
                }

                Collection<InstanceSpec> instances = newCluster.getInstances();
                client.reconfig()
                        .leaving(Integer.toString(removeSpec.getServerId()))
                        .joining(toReconfigSpec(instances))
                        .fromConfig(oldConfig.getVersion())
                        .forEnsemble();

                assertTrue(timing.awaitLatch(eventLatch));
                assertTrue(timing.awaitLatch(ensembleLatch));

                byte[] newConfigData = client.getConfig().forEnsemble();
                QuorumVerifier newConfig = toQuorumVerifier(newConfigData);
                ArrayList<InstanceSpec> newInstances = Lists.newArrayList(oldInstances);
                newInstances.addAll(instances);
                newInstances.remove(removeSpec);
                assertConfig(newConfig, newInstances);
                assertEquals(
                        EnsembleTracker.configToConnectionString(newConfig), ensembleProvider.getConnectionString());
            }
        }
    }

    @Test
    public void testAddAndRemoveWithEmptyList() throws Exception {
        try (CuratorFramework client = newClient()) {
            client.start();

            QuorumVerifier oldConfig = toQuorumVerifier(client.getConfig().forEnsemble());
            assertConfig(oldConfig, cluster.getInstances());

            CountDownLatch latch = setChangeWaiter(client);

            Collection<InstanceSpec> oldInstances = cluster.getInstances();
            client.reconfig()
                    .leaving(Collections.emptyList())
                    .joining(Collections.emptyList())
                    .fromConfig(oldConfig.getVersion())
                    .forEnsemble();

            assertTrue(timing.awaitLatch(latch));

            byte[] newConfigData = client.getConfig().forEnsemble();
            QuorumVerifier newConfig = toQuorumVerifier(newConfigData);
            assertConfig(newConfig, oldInstances);
            assertEquals(EnsembleTracker.configToConnectionString(newConfig), ensembleProvider.getConnectionString());
        }
    }

    @Test
    public void testNewMembersWithEmptyList() throws Exception {
        try (CuratorFramework client = newClient()) {
            client.start();

            QuorumVerifier oldConfig = toQuorumVerifier(client.getConfig().forEnsemble());
            assertConfig(oldConfig, cluster.getInstances());

            CountDownLatch latch = setChangeWaiter(client);

            Collection<InstanceSpec> oldInstances = cluster.getInstances();
            client.reconfig()
                    .withNewMembers(Collections.emptyList())
                    .fromConfig(oldConfig.getVersion())
                    .forEnsemble();

            assertTrue(timing.awaitLatch(latch));

            byte[] newConfigData = client.getConfig().forEnsemble();
            QuorumVerifier newConfig = toQuorumVerifier(newConfigData);
            assertConfig(newConfig, oldInstances);
            assertEquals(EnsembleTracker.configToConnectionString(newConfig), ensembleProvider.getConnectionString());
        }
    }

    @Test
    @Disabled // it's what this test is inteded to do and it keeps failing - disable for now
    public void testNewMembers() throws Exception {
        cluster.close();
        cluster = null;

        TestingCluster smallCluster = null;
        TestingCluster localCluster = new TestingCluster(5);
        try {
            List<TestingZooKeeperServer> servers = localCluster.getServers();
            List<InstanceSpec> smallClusterInstances = Lists.newArrayList();
            for (int i = 0; i < 3; ++i) // only start 3 of the 5
            {
                TestingZooKeeperServer server = servers.get(i);
                server.start();
                smallClusterInstances.add(server.getInstanceSpec());
            }

            smallCluster = new TestingCluster(smallClusterInstances);
            try (CuratorFramework client = newClient(smallCluster.getConnectString())) {
                client.start();

                QuorumVerifier oldConfig = toQuorumVerifier(client.getConfig().forEnsemble());
                assertEquals(oldConfig.getAllMembers().size(), 5);
                assertConfig(oldConfig, localCluster.getInstances());

                CountDownLatch latch = setChangeWaiter(client);

                client.reconfig()
                        .withNewMembers(toReconfigSpec(smallClusterInstances))
                        .forEnsemble();

                assertTrue(timing.awaitLatch(latch));
                byte[] newConfigData = client.getConfig().forEnsemble();
                QuorumVerifier newConfig = toQuorumVerifier(newConfigData);
                assertEquals(newConfig.getAllMembers().size(), 3);
                assertConfig(newConfig, smallClusterInstances);
                assertEquals(
                        EnsembleTracker.configToConnectionString(newConfig), ensembleProvider.getConnectionString());
            }
        } finally {
            CloseableUtils.closeQuietly(smallCluster);
            CloseableUtils.closeQuietly(localCluster);
        }
    }

    @Test
    public void testRemoveWithChroot() throws Exception {
        // Use a long chroot path to circumvent ZOOKEEPER-4565 and ZOOKEEPER-4601
        String chroot = "/pretty-long-chroot";
        CountDownLatch ensembleLatch = new CountDownLatch(1);

        try (CuratorFramework client = newClient(cluster.getConnectString() + chroot, ensembleLatch)) {
            client.start();
            client.create().forPath("/", "deadbeef".getBytes());

            QuorumVerifier oldConfig = toQuorumVerifier(client.getConfig().forEnsemble());
            assertConfig(oldConfig, cluster.getInstances());

            CountDownLatch latch = setChangeWaiter(client);

            Collection<InstanceSpec> oldInstances = cluster.getInstances();
            InstanceSpec us =
                    cluster.findConnectionInstance(client.getZookeeperClient().getZooKeeper());
            InstanceSpec removeSpec = oldInstances.iterator().next();
            if (us.equals(removeSpec)) {
                Iterator<InstanceSpec> iterator = oldInstances.iterator();
                iterator.next();
                removeSpec = iterator.next();
            }

            client.reconfig()
                    .leaving(Integer.toString(removeSpec.getServerId()))
                    .fromConfig(oldConfig.getVersion())
                    .forEnsemble();

            assertTrue(timing.awaitLatch(latch));

            byte[] newConfigData = client.getConfig().forEnsemble();
            QuorumVerifier newConfig = toQuorumVerifier(newConfigData);
            List<InstanceSpec> newInstances = Lists.newArrayList(cluster.getInstances());
            newInstances.remove(removeSpec);
            assertConfig(newConfig, newInstances);

            assertTrue(timing.awaitLatch(ensembleLatch));
            String connectString = EnsembleTracker.configToConnectionString(newConfig) + chroot;
            assertEquals(connectString, ensembleProvider.getConnectionString());

            client.getZookeeperClient().reset();
            client.sync().forPath("/");
            byte[] data = client.getData().forPath("/");
            assertThat(data).asString().isEqualTo("deadbeef");
        }
    }

    @Test
    public void testConfigToConnectionStringIPv4Normal() throws Exception {
        String config = "server.1=10.1.2.3:2888:3888:participant;10.2.3.4:2181";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("10.2.3.4:2181", configString);
    }

    @Test
    public void testConfigToConnectionStringIPv6Normal() throws Exception {
        String config =
                "server.1=[1010:0001:0002:0003:0004:0005:0006:0007]:2888:3888:participant;[2001:db8:85a3:0:0:8a2e:370:7334]:2181";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("2001:db8:85a3:0:0:8a2e:370:7334:2181", configString);
    }

    @Test
    public void testConfigToConnectionStringIPv4NoClientAddr() throws Exception {
        String config = "server.1=10.1.2.3:2888:3888:participant;2181";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("10.1.2.3:2181", configString);
    }

    @Test
    public void testConfigToConnectionStringIPv4WildcardClientAddr() throws Exception {
        String config = "server.1=10.1.2.3:2888:3888:participant;0.0.0.0:2181";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("10.1.2.3:2181", configString);
    }

    @Test
    public void testConfigToConnectionStringNoClientAddrOrPort() throws Exception {
        String config = "server.1=10.1.2.3:2888:3888:participant";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("", configString);
    }

    @Test
    public void testConfigToConnectionStringPreservesHostnameNormal() throws Exception {
        String config = "server.1=server.addr:2888:3888:participant;client.addr:2181";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("client.addr:2181", configString);
    }

    @Test
    public void testConfigToConnectionStringPreservesHostnameNoClientAddr() throws Exception {
        String config = "server.1=server.addr:2888:3888:participant;2181";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("server.addr:2181", configString);
    }

    @Test
    public void testConfigToConnectionStringPreservesHostnameIPv4Wildcard() throws Exception {
        String config = "server.1=server.addr:2888:3888:participant;0.0.0.0:2181";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("server.addr:2181", configString);
    }

    @Test
    public void testConfigToConnectionStringPreservesHostnameIPv6Wildcard() throws Exception {
        String config = "server.1=server.addr:2888:3888:participant;[::]:2181";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("server.addr:2181", configString);
    }

    @Test
    public void testIPv6Wildcard1() throws Exception {
        String config = "server.1=[2001:db8:85a3:0:0:8a2e:370:7334]:2888:3888:participant;[::]:2181";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("2001:db8:85a3:0:0:8a2e:370:7334:2181", configString);
    }

    @Test
    public void testIPv6Wildcard2() throws Exception {
        String config = "server.1=[1010:0001:0002:0003:0004:0005:0006:0007]:2888:3888:participant;[::0]:2181";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("1010:1:2:3:4:5:6:7:2181", configString);
    }

    @Test
    public void testMixedIPv1() throws Exception {
        String config = "server.1=10.1.2.3:2888:3888:participant;[::]:2181";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("10.1.2.3:2181", configString);
    }

    @Test
    public void testMixedIPv2() throws Exception {
        String config = "server.1=[2001:db8:85a3:0:0:8a2e:370:7334]:2888:3888:participant;127.0.0.1:2181";
        String configString = EnsembleTracker.configToConnectionString(toQuorumVerifier(config.getBytes()));
        assertEquals("127.0.0.1:2181", configString);
    }

    @Override
    protected void createServer() throws Exception {
        // NOP
    }

    private CuratorFramework newClient() {
        return newClient(cluster.getConnectString(), true);
    }

    private CuratorFramework newClient(String connectionString) {
        return newClient(connectionString, true);
    }

    private CuratorFramework newClient(String connectionString, boolean withEnsembleTracker) {
        return newClient(connectionString, withEnsembleTracker, null);
    }

    private CuratorFramework newClient(String connectionString, CountDownLatch ensembleLatch) {
        return newClient(connectionString, ensembleLatch != null, ensembleLatch);
    }

    private CuratorFramework newClient(
            String connectionString, boolean withEnsembleTracker, CountDownLatch ensembleLatch) {
        final AtomicReference<String> connectString = new AtomicReference<>(connectionString);
        ensembleProvider = new EnsembleProvider() {
            @Override
            public void start() throws Exception {}

            @Override
            public boolean updateServerListEnabled() {
                return false;
            }

            @Override
            public String getConnectionString() {
                return connectString.get();
            }

            @Override
            public void close() throws IOException {}

            @Override
            public void setConnectionString(String connectionString) {
                if (!connectionString.equals(getConnectionString())) {
                    connectString.set(connectionString);
                    if (ensembleLatch != null) {
                        ensembleLatch.countDown();
                    }
                }
            }
        };
        return CuratorFrameworkFactory.builder()
                .ensembleProvider(ensembleProvider)
                .ensembleTracker(withEnsembleTracker)
                .sessionTimeoutMs(timing.session())
                .connectionTimeoutMs(timing.connection())
                .authorization("digest", superUserPassword.getBytes())
                .retryPolicy(
                        new ExponentialBackoffRetry(timing.forSleepingABit().milliseconds(), 3))
                .build();
    }

    private CountDownLatch setChangeWaiter(CuratorFramework client) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    latch.countDown();
                }
            }
        };
        client.getConfig().usingWatcher(watcher).forEnsemble();
        return latch;
    }

    private void assertConfig(QuorumVerifier config, Collection<InstanceSpec> instances) {
        for (InstanceSpec instance : instances) {
            QuorumPeer.QuorumServer quorumServer = config.getAllMembers().get((long) instance.getServerId());
            assertNotNull(
                    quorumServer,
                    String.format("Looking for %s - found %s", instance.getServerId(), config.getAllMembers()));
            assertEquals(quorumServer.clientAddr.getPort(), instance.getPort());
        }
    }

    private List<String> toReconfigSpec(Collection<InstanceSpec> instances) throws Exception {
        String localhost =
                new InetSocketAddress((InetAddress) null, 0).getAddress().getHostAddress();
        List<String> specs = Lists.newArrayList();
        for (InstanceSpec instance : instances) {
            specs.add("server." + instance.getServerId() + "=" + localhost + ":" + instance.getElectionPort() + ":"
                    + instance.getQuorumPort() + ";" + instance.getPort());
        }
        return specs;
    }

    private static QuorumVerifier toQuorumVerifier(byte[] bytes) throws Exception {
        assertNotNull(bytes);
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(bytes));
        return new QuorumMaj(properties);
    }
}
