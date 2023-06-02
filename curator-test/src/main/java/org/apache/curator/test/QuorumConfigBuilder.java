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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.curator.test.compatibility.Timing2;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

@SuppressWarnings("UnusedDeclaration")
public class QuorumConfigBuilder implements Closeable {
    private final ImmutableList<InstanceSpec> instanceSpecs;
    private final boolean fromRandom;
    private final File fakeConfigFile;

    public QuorumConfigBuilder(Collection<InstanceSpec> specs) {
        this(specs.toArray(new InstanceSpec[0]));
    }

    public QuorumConfigBuilder(InstanceSpec... specs) {
        fromRandom = (specs == null) || (specs.length == 0);
        instanceSpecs = fromRandom ? ImmutableList.of(InstanceSpec.newInstanceSpec()) : ImmutableList.copyOf(specs);
        File fakeConfigFile = null;
        try {
            fakeConfigFile = File.createTempFile("temp", "temp");
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        this.fakeConfigFile = fakeConfigFile;
    }

    public boolean isFromRandom() {
        return fromRandom;
    }

    public QuorumPeerConfig buildConfig() throws Exception {
        return buildConfig(0);
    }

    public InstanceSpec getInstanceSpec(int index) {
        return instanceSpecs.get(index);
    }

    public List<InstanceSpec> getInstanceSpecs() {
        return instanceSpecs;
    }

    public int size() {
        return instanceSpecs.size();
    }

    @Override
    public void close() {
        if (fakeConfigFile != null) {
            //noinspection ResultOfMethodCallIgnored
            fakeConfigFile.delete();
        }
    }

    public QuorumPeerConfig buildConfig(int instanceIndex) throws Exception {
        InstanceSpec spec = instanceSpecs.get(instanceIndex);
        return buildConfig(instanceIndex, spec.getPort());
    }

    public QuorumPeerConfig buildConfig(int instanceIndex, int instancePort) throws Exception {
        Properties properties = buildConfigProperties(instanceIndex, instancePort);
        QuorumPeerConfig config = new QuorumPeerConfig() {
            {
                if (fakeConfigFile != null) {
                    configFileStr = fakeConfigFile.getPath();
                }
            }
        };
        config.parseProperties(properties);
        return config;
    }

    public Properties buildConfigProperties(int instanceIndex) throws Exception {
        InstanceSpec spec = instanceSpecs.get(instanceIndex);
        return buildConfigProperties(instanceIndex, spec.getPort());
    }

    public Properties buildConfigProperties(int instanceIndex, int instancePort) throws Exception {
        boolean isCluster = (instanceSpecs.size() > 1);
        InstanceSpec spec = instanceSpecs.get(instanceIndex);

        if (isCluster) {
            Files.write(Integer.toString(spec.getServerId()).getBytes(), new File(spec.getDataDirectory(), "myid"));
        }

        Properties properties = new Properties();
        String localSessionsEnabled = System.getProperty("readonlymode.enabled", "false");
        properties.setProperty("localSessionsEnabled", localSessionsEnabled);
        properties.setProperty("localSessionsUpgradingEnabled", localSessionsEnabled);
        properties.setProperty("initLimit", "10");
        properties.setProperty("syncLimit", "5");
        properties.setProperty("dataDir", spec.getDataDirectory().getCanonicalPath());
        properties.setProperty("clientPort", Integer.toString(instancePort));
        String tickTime = Integer.toString((spec.getTickTime() >= 0) ? spec.getTickTime() : new Timing2().tickTime());
        properties.setProperty("tickTime", tickTime);
        properties.setProperty("minSessionTimeout", tickTime);
        int maxClientCnxns = spec.getMaxClientCnxns();
        if (maxClientCnxns >= 0) {
            properties.setProperty("maxClientCnxns", Integer.toString(maxClientCnxns));
        }

        if (isCluster) {
            for (InstanceSpec thisSpec : instanceSpecs) {
                int clientPort = thisSpec == spec ? instancePort : thisSpec.getPort();
                properties.setProperty(
                        "server." + thisSpec.getServerId(),
                        String.format(
                                "%s:%d:%d;%s:%d",
                                thisSpec.getHostname(),
                                thisSpec.getQuorumPort(),
                                thisSpec.getElectionPort(),
                                thisSpec.getHostname(),
                                clientPort));
            }
        }
        Map<String, Object> customProperties = spec.getCustomProperties();
        if (customProperties != null) {
            properties.putAll(customProperties);
        }

        return properties;
    }

    public QuorumPeerConfigBuilder bindInstance(int instanceIndex, int instancePort) {
        return new QuorumPeerConfigBuilder(this, instanceIndex, instancePort);
    }
}
