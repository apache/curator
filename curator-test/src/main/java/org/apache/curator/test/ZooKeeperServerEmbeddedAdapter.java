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

package org.apache.curator.test;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.embedded.ZooKeeperServerEmbedded;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.apache.zookeeper.server.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperServerEmbeddedAdapter implements ZooKeeperMainFace {
    private static final Logger log = LoggerFactory.getLogger(ZooKeeperServerEmbeddedAdapter.class);
    private static final Duration DEFAULT_STARTUP_TIMEOUT = Duration.ofMinutes(1);

    private volatile ZooKeeperServerEmbedded zooKeeperEmbedded;

    @Override
    public void start(QuorumPeerConfigBuilder configBuilder) {
        try {
            final Properties properties = configBuilder.buildProperties();
            properties.put("admin.enableServer", "false");
            properties.put("4lw.commands.whitelist", "*");

            final Path dataDir = Paths.get(properties.getProperty("dataDir"));
            zooKeeperEmbedded = ZooKeeperServerEmbedded.builder()
                    .configuration(properties)
                    .baseDir(dataDir.getParent())
                    .build();
            log.info("Configure ZooKeeperServerEmbeddedAdapter with properties: {}", properties);

            // Before ZOOKEEPER-4303, there are issues when setting "clientPort" to 0:
            // * It does not set "clientPortAddress" which causes ZooKeeper started with no
            //   server cnxn factory to serve client requests.
            // * It uses "clientPortAddress" to construct connection string but not bound port.
            //
            // So here, we hijack start process to circumvent these if there is no fix applied.
            // * Setup "clientPortAddress" if it is null.
            // * Setup "clientPortAddress" with bound port after started if above step applied.
            if (hijackClientPort(0)) {
                zooKeeperEmbedded.start(DEFAULT_STARTUP_TIMEOUT.toMillis());
                int port = getServerCnxnFactory().getLocalPort();
                hijackClientPort(port);
            } else {
                zooKeeperEmbedded.start(DEFAULT_STARTUP_TIMEOUT.toMillis());
            }
        } catch (Exception e) {
            throw new FailedServerStartException(e);
        }
    }

    @Override
    public int getClientPort() throws Exception {
        String address = zooKeeperEmbedded.getConnectionString();
        try {
            String[] parts = ConfigUtils.getHostAndPort(address);
            return Integer.parseInt(parts[1], 10);
        } catch (Exception ex) {
            throw new IllegalStateException("invalid connection string: " + address);
        }
    }

    private boolean hijackClientPort(int port) {
        try {
            Class<?> clazz = Class.forName("org.apache.zookeeper.server.embedded.ZooKeeperServerEmbeddedImpl");
            Field configField = clazz.getDeclaredField("config");
            configField.setAccessible(true);
            QuorumPeerConfig peerConfig = (QuorumPeerConfig) configField.get(zooKeeperEmbedded);
            if (peerConfig.getClientPortAddress() == null || port != 0) {
                Field addressField = QuorumPeerConfig.class.getDeclaredField("clientPortAddress");
                addressField.setAccessible(true);
                addressField.set(peerConfig, new InetSocketAddress(port));
                return true;
            }
        } catch (Exception e) {
            // swallow hijack failure to accommodate possible upstream changes
            log.debug("Failed to hijack client port configuration. This may be due to upstream changes", e);
        }
        return false;
    }

    public ServerCnxnFactory getServerCnxnFactory() {
        try {
            Class<?> clazz = Class.forName("org.apache.zookeeper.server.embedded.ZooKeeperServerEmbeddedImpl");
            Field clusterField = clazz.getDeclaredField("maincluster");
            clusterField.setAccessible(true);
            QuorumPeerMain quorumPeerMain = (QuorumPeerMain) clusterField.get(zooKeeperEmbedded);
            if (quorumPeerMain != null) {
                Field quorumPeerField = QuorumPeerMain.class.getDeclaredField("quorumPeer");
                quorumPeerField.setAccessible(true);
                QuorumPeer quorumPeer = (QuorumPeer) quorumPeerField.get(quorumPeerMain);
                return getServerCnxnFactory(QuorumPeer.class, quorumPeer, "cnxnFactory");
            }
            Field serverField = clazz.getDeclaredField("mainsingle");
            serverField.setAccessible(true);
            ZooKeeperServerMain server = (ZooKeeperServerMain) serverField.get(zooKeeperEmbedded);
            return getServerCnxnFactory(ZooKeeperServerMain.class, server, "cnxnFactory");
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException ex) {
            throw new IllegalStateException("zk server cnxn factory not found", ex);
        }
    }

    static ServerCnxnFactory getServerCnxnFactory(Class<?> clazz, Object obj, String fieldName)
            throws NoSuchFieldException, IllegalAccessException {
        Field cnxnFactoryField = clazz.getDeclaredField(fieldName);
        cnxnFactoryField.setAccessible(true);
        return (ServerCnxnFactory) cnxnFactoryField.get(obj);
    }

    @Override
    public void kill() {
        close();
    }

    @Override
    public void close() {
        if (zooKeeperEmbedded != null) {
            zooKeeperEmbedded.close();
        }
    }
}
