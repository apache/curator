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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import org.apache.zookeeper.server.embedded.ZooKeeperServerEmbedded;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperServerEmbeddedAdapter implements ZooKeeperMainFace {
    private static final Logger log = LoggerFactory.getLogger(ZooKeeperServerEmbeddedAdapter.class);
    private static final Duration DEFAULT_STARTUP_TIMEOUT = Duration.ofMinutes(1);

    private volatile ZooKeeperServerEmbedded zooKeeperEmbedded;
    private volatile QuorumConfigBuilder configBuilder;
    private volatile int instanceIndex;

    @Override
    public void configure(QuorumConfigBuilder config, int instanceIndex) throws Exception {
        this.configBuilder = config;
        this.instanceIndex = instanceIndex;

        final Properties properties = config.buildConfigProperties(instanceIndex);
        properties.put("admin.enableServer", "false");

        final Path dataDir = Paths.get(properties.getProperty("dataDir"));
        zooKeeperEmbedded = ZooKeeperServerEmbedded.builder()
                .configuration(properties)
                .baseDir(dataDir.getParent())
                .build();
        log.info("Configure ZooKeeperServerEmbeddedAdapter with properties: {}", properties);
    }

    @Override
    public QuorumPeerConfig getConfig() throws Exception {
        if (configBuilder != null) {
            return configBuilder.buildConfig(instanceIndex);
        }

        return null;
    }

    @Override
    public void start() {
        if (zooKeeperEmbedded == null) {
            throw new FailedServerStartException(new NullPointerException("zooKeeperEmbedded"));
        }

        try {
            zooKeeperEmbedded.start(DEFAULT_STARTUP_TIMEOUT.toMillis());
        } catch (Exception e) {
            throw new FailedServerStartException(e);
        }
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
