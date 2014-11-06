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
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TestReconfiguration {

    TestingCluster cluster;

    @BeforeMethod
    public void setup() throws Exception {
        cluster = new TestingCluster(5);
        cluster.start();
    }

    @AfterMethod
    public void tearDown() throws IOException {
        cluster.close();
    }

    @Test
    public void testSyncIncremental() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryOneTime(1));
        client.start();
        client.blockUntilConnected();
        try {
            Stat stat = new Stat();
            byte[] bytes = client.getConfig().storingStatIn(stat).forEnsemble();
            Assert.assertNotNull(bytes);
            QuorumVerifier qv = getQuorumVerifier(bytes);
            Assert.assertEquals(qv.getAllMembers().size(), 5);
            String server1 = getServerString(qv, cluster, 1L);
            String server2 = getServerString(qv, cluster, 2L);

            //Remove Servers
            bytes = client.reconfig().leaving("1").storingStatIn(stat).fromConfig(qv.getVersion()).forEnsemble();
            qv = getQuorumVerifier(bytes);
            Assert.assertEquals(qv.getAllMembers().size(), 4);
            bytes = client.reconfig().leaving("2").storingStatIn(stat).fromConfig(qv.getVersion()).forEnsemble();
            qv = getQuorumVerifier(bytes);
            Assert.assertEquals(qv.getAllMembers().size(), 3);

            //Add Servers
            bytes = client.reconfig().joining("server.1=" + server1).storingStatIn(stat).fromConfig(qv.getVersion()).forEnsemble();
            qv = getQuorumVerifier(bytes);
            Assert.assertEquals(qv.getAllMembers().size(), 4);
            bytes = client.reconfig().joining("server.2=" + server2).storingStatIn(stat).fromConfig(qv.getVersion()).forEnsemble();
            qv = getQuorumVerifier(bytes);
            Assert.assertEquals(qv.getAllMembers().size(), 5);
        } finally {
            client.close();
        }
    }

    @Test
    public void testAsyncIncremental() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryOneTime(1));
        client.start();
        client.blockUntilConnected();
        try {
            final AtomicReference<byte[]> bytes = new AtomicReference<byte[]>();
            final BackgroundCallback callback = new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    bytes.set(event.getData());
                    ((CountDownLatch)event.getContext()).countDown();
                }

            };

            CountDownLatch latch = new CountDownLatch(1);
            client.getConfig().inBackground(callback, latch).forEnsemble();
            latch.await(5, TimeUnit.SECONDS);
            Assert.assertNotNull(bytes.get());
            QuorumVerifier qv = getQuorumVerifier(bytes.get());
            Assert.assertEquals(qv.getAllMembers().size(), 5);
            String server1 = getServerString(qv, cluster, 1L);
            String server2 = getServerString(qv, cluster, 2L);


            //Remove Servers
            latch = new CountDownLatch(1);
            client.reconfig().leaving("1").inBackground(callback, latch).fromConfig(qv.getVersion()).forEnsemble();
            latch.await(5, TimeUnit.SECONDS);
            qv = getQuorumVerifier(bytes.get());
            Assert.assertEquals(qv.getAllMembers().size(), 4);
            latch = new CountDownLatch(1);
            client.reconfig().leaving("2").inBackground(callback, latch).fromConfig(qv.getVersion()).forEnsemble();
            latch.await(5, TimeUnit.SECONDS);
            qv = getQuorumVerifier(bytes.get());
            Assert.assertEquals(qv.getAllMembers().size(), 3);

            //Add Servers
            latch = new CountDownLatch(1);
            client.reconfig().joining("server.1=" + server1).inBackground(callback, latch).fromConfig(qv.getVersion()).forEnsemble();
            latch.await(5, TimeUnit.SECONDS);
            qv = getQuorumVerifier(bytes.get());
            Assert.assertEquals(qv.getAllMembers().size(), 4);
            latch = new CountDownLatch(1);
            client.reconfig().joining("server.2=" + server2).inBackground(callback, latch).fromConfig(qv.getVersion()).forEnsemble();
            latch.await(5, TimeUnit.SECONDS);
            qv = getQuorumVerifier(bytes.get());
            Assert.assertEquals(qv.getAllMembers().size(), 5);
        } finally {
            client.close();
        }
    }

    @Test
    public void testSyncNonIncremental() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryOneTime(1));
        client.start();
        client.blockUntilConnected();
        try {
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
                    .withMembers("server.2=" + server2,
                            "server.3=" + server3,
                            "server.4=" + server4,
                            "server.5=" + server5)
                    .storingStatIn(stat).fromConfig(qv.getVersion()).forEnsemble();
            qv = getQuorumVerifier(bytes);
            Assert.assertEquals(qv.getAllMembers().size(), 4);
            bytes = client.reconfig()
                    .withMembers("server.3=" + server3,
                            "server.4=" + server4,
                            "server.5=" + server5)
                    .storingStatIn(stat).fromConfig(qv.getVersion()).forEnsemble();

            qv = getQuorumVerifier(bytes);
            Assert.assertEquals(qv.getAllMembers().size(), 3);

            //Add Servers
            bytes = client.reconfig()
                    .withMembers("server.1=" + server1,
                            "server.3=" + server3,
                            "server.4=" + server4,
                            "server.5=" + server5)
                    .storingStatIn(stat).fromConfig(qv.getVersion()).forEnsemble();
            qv = getQuorumVerifier(bytes);
            Assert.assertEquals(qv.getAllMembers().size(), 4);
            bytes = client.reconfig()
                    .withMembers("server.1=" + server1,
                            "server.2=" + server2,
                            "server.3=" + server3,
                            "server.4=" + server4,
                            "server.5=" + server5)
                    .storingStatIn(stat).fromConfig(qv.getVersion()).forEnsemble();
            qv = getQuorumVerifier(bytes);
            Assert.assertEquals(qv.getAllMembers().size(), 5);
        } finally {
            client.close();
        }
    }

    @Test
    public void testAsyncNonIncremental() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new RetryOneTime(1));
        client.start();
        client.blockUntilConnected();
        try {
            final AtomicReference<byte[]> bytes = new AtomicReference<byte[]>();
            final BackgroundCallback callback = new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    bytes.set(event.getData());
                    ((CountDownLatch)event.getContext()).countDown();
                }

            };

            CountDownLatch latch = new CountDownLatch(1);
            client.getConfig().inBackground(callback, latch).forEnsemble();
            latch.await(5, TimeUnit.SECONDS);
            Assert.assertNotNull(bytes.get());
            QuorumVerifier qv = getQuorumVerifier(bytes.get());
            Assert.assertEquals(qv.getAllMembers().size(), 5);
            String server1 = getServerString(qv, cluster, 1L);
            String server2 = getServerString(qv, cluster, 2L);
            String server3 = getServerString(qv, cluster, 3L);
            String server4 = getServerString(qv, cluster, 4L);
            String server5 = getServerString(qv, cluster, 5L);

            //Remove Servers
            latch = new CountDownLatch(1);
            client.reconfig()
                    .withMembers("server.2=" + server2,
                            "server.3=" + server3,
                            "server.4=" + server4,
                            "server.5=" + server5)
            .inBackground(callback, latch).fromConfig(qv.getVersion()).forEnsemble();
            latch.await(5, TimeUnit.SECONDS);
            qv = getQuorumVerifier(bytes.get());
            Assert.assertEquals(qv.getAllMembers().size(), 4);
            latch = new CountDownLatch(1);
            client.reconfig()
                    .withMembers("server.3=" + server3,
                            "server.4=" + server4,
                            "server.5=" + server5)
                    .inBackground(callback, latch).fromConfig(qv.getVersion()).forEnsemble();
            latch.await(5, TimeUnit.SECONDS);
            qv = getQuorumVerifier(bytes.get());
            Assert.assertEquals(qv.getAllMembers().size(), 3);

            //Add Servers
            latch = new CountDownLatch(1);
            client.reconfig()
                    .withMembers("server.1=" + server1,
                            "server.3=" + server3,
                            "server.4=" + server4,
                            "server.5=" + server5)
                    .inBackground(callback, latch).fromConfig(qv.getVersion()).forEnsemble();
            latch.await(5, TimeUnit.SECONDS);
            qv = getQuorumVerifier(bytes.get());
            Assert.assertEquals(qv.getAllMembers().size(), 4);
            latch = new CountDownLatch(1);
            client.reconfig()
                    .withMembers("server.1=" + server1,
                            "server.2=" + server2,
                            "server.3=" + server3,
                            "server.4=" + server4,
                            "server.5=" + server5)
                    .inBackground(callback, latch).fromConfig(qv.getVersion()).forEnsemble();
            latch.await(5, TimeUnit.SECONDS);
            qv = getQuorumVerifier(bytes.get());
            Assert.assertEquals(qv.getAllMembers().size(), 5);
        } finally {
            client.close();
        }
    }


    static QuorumVerifier getQuorumVerifier(byte[] bytes) throws Exception {
        Properties properties = new Properties();
        properties.load(new StringReader(new String(bytes)));
        return new QuorumMaj(properties);
    }

    static InstanceSpec getInstance(TestingCluster cluster, int id) {
        for (InstanceSpec spec : cluster.getInstances()) {
            if (spec.getServerId() == id) {
                return spec;
            }
        }
        throw new IllegalStateException("InstanceSpec with id:" + id + " not found");
    }

    static String getServerString(QuorumVerifier qv, TestingCluster cluster, long id) throws Exception {
        String str = qv.getAllMembers().get(id).toString();
        //check if connection string is already there.
        if (str.contains(";")) {
            return str;
        } else {
            return str + ";" + getInstance(cluster, (int) id).getConnectString();
        }
    }
}