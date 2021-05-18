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

package org.apache.curator.test;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.embedded.ZooKeeperServerEmbedded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ZooKeeperEmbeddedRunner implements ZooKeeperMainFace
{
    private static final Logger log = LoggerFactory.getLogger(ZooKeeperEmbeddedRunner.class);
    private ZooKeeperServerEmbedded zooKeeperEmbedded;
    private String address;

    @Override
    public void kill()
    {
        if (zooKeeperEmbedded == null) {
            return;
        }
        zooKeeperEmbedded.close();
    }

    @Override
    public void configure(QuorumConfigBuilder config, int instance) {
        try {
            Properties properties = config.buildRawConfig(instance);
            properties.put("admin.enableServer", "false");
            Path dataDir = Paths.get(properties.getProperty("dataDir"));
            zooKeeperEmbedded = ZooKeeperServerEmbedded
                    .builder()
                    .configuration(properties)
                    .baseDir(dataDir.getParent())
                    .build();
            address = properties.getProperty("clientPortAddress", "0.0.0.0") + ":" + properties.getProperty("clientPort", "2181");
            log.info("ZK configuration is {}", properties);
            log.info("ZK address is {}", address);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() {
        if (zooKeeperEmbedded == null) {
            throw new IllegalStateException();
        }
        try {
            zooKeeperEmbedded.start();
            waitForServer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void waitForServer() throws Exception {
        Thread.sleep(1000);
        CompletableFuture<Object> waitForConnection = new CompletableFuture<>();
        try (ZooKeeper zk = new ZooKeeper(address, 60000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                log.info("event {}", event);
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    waitForConnection.complete(event);
                }
            }
        });) {
            try {
                waitForConnection.get(120, TimeUnit.SECONDS);
            } catch (TimeoutException err) {
                throw new RuntimeException("Server at " + address + " did not start", err);
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
                throw err;
            }
            // wait for resources to be disposed
            zk.close(1000);
        }
    }

    @Override
    public void close() throws IOException
    {
        kill();
    }
}
