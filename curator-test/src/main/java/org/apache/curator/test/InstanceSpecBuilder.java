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

import java.io.File;
import java.util.Map;

/**
 * Builder to construct {@link InstanceSpec}.
 */
public class InstanceSpecBuilder {
    private File dataDirectory;
    private boolean deleteDataDirectoryOnClose = true;
    private int port = -1;
    private int electionPort = -1;
    private int quorumPort = -1;
    private int serverId = -1;
    private int tickTime = -1;
    private int maxClientCnxns = -1;
    private Map<String, String> customProperties = null;
    private String hostname = null;

    /**
     * Data directory to store data of this server instance. It will be deleted upon sever close.
     *
     * @see #withDataDirectory(File, boolean) also
     */
    public InstanceSpecBuilder withDataDirectory(File dataDirectory) {
        this.dataDirectory = dataDirectory;
        return this;
    }

    public InstanceSpecBuilder withDataDirectory(File dataDirectory, boolean deleteDataDirectoryOnClose) {
        this.dataDirectory = dataDirectory;
        this.deleteDataDirectoryOnClose = deleteDataDirectoryOnClose;
        return this;
    }

    public InstanceSpecBuilder withPort(int port) {
        this.port = port;
        return this;
    }

    public InstanceSpecBuilder withElectionPort(int electionPort) {
        this.electionPort = electionPort;
        return this;
    }

    public InstanceSpecBuilder withQuorumPort(int quorumPort) {
        this.quorumPort = quorumPort;
        return this;
    }

    public InstanceSpecBuilder withServerId(int serverId) {
        this.serverId = serverId;
        return this;
    }

    public InstanceSpecBuilder withTickTime(int tickTime) {
        this.tickTime = tickTime;
        return this;
    }

    public InstanceSpecBuilder withMaxClientCnxns(int maxClientCnxns) {
        this.maxClientCnxns = maxClientCnxns;
        return this;
    }

    public InstanceSpecBuilder withCustomProperties(Map<String, String> customProperties) {
        this.customProperties = customProperties;
        return this;
    }

    public InstanceSpecBuilder withHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    @SuppressWarnings("unchecked")
    public InstanceSpec build() {
        return new InstanceSpec(
                dataDirectory,
                port,
                electionPort,
                quorumPort,
                deleteDataDirectoryOnClose,
                serverId,
                tickTime,
                maxClientCnxns,
                (Map) customProperties,
                hostname,
                true);
    }
}
