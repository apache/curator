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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thanks to Jeremie BORDIER (ahfeel) for this code
 */
public class TestingZooKeeperServer implements Closeable
{
    private static final Logger log = LoggerFactory.getLogger(TestingZooKeeperServer.class);
    static boolean hasZooKeeperServerEmbedded;

    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final QuorumConfigBuilder configBuilder;
    private final int thisInstanceIndex;
    private int thisInstancePort;
    private volatile ZooKeeperMainFace main;

    static {
        boolean localHasZooKeeperServerEmbedded;
        try {
            Class.forName("org.apache.zookeeper.server.embedded.ZooKeeperServerEmbedded");
            localHasZooKeeperServerEmbedded = true;
        } catch (Throwable t) {
            localHasZooKeeperServerEmbedded = false;
            log.info("ZooKeeperServerEmbedded is not available in the version of the ZooKeeper library being used");
        }
        hasZooKeeperServerEmbedded = localHasZooKeeperServerEmbedded;
    }

    ZooKeeperMainFace getMain() {
        return main;
    }

    private enum State
    {
        LATENT, STARTED, STOPPED, CLOSED
    }

    public TestingZooKeeperServer(QuorumConfigBuilder configBuilder)
    {
        this(configBuilder, 0);
    }

    public TestingZooKeeperServer(QuorumConfigBuilder configBuilder, int thisInstanceIndex)
    {
        System.setProperty("zookeeper.jmx.log4j.disable", "true");  // disable JMX logging

        this.configBuilder = configBuilder;
        this.thisInstanceIndex = thisInstanceIndex;
        this.thisInstancePort = configBuilder.getInstanceSpec(thisInstanceIndex).getPort();
        main = createServerMain();
    }

    private ZooKeeperMainFace createServerMain() {
        if (hasZooKeeperServerEmbedded) {
            return new ZooKeeperServerEmbeddedAdapter();
        } else if (isCluster()) {
            return new TestingQuorumPeerMain();
        } else {
            return new TestingZooKeeperMain();
        }
    }

    private boolean isCluster() {
        return configBuilder.size() > 1;
    }

    public Collection<InstanceSpec> getInstanceSpecs()
    {
        return configBuilder.getInstanceSpecs();
    }

    public void kill()
    {
        main.kill();
        state.set(State.STOPPED);
    }

    /**
     * Restart the server. If the server is running it will be stopped and then
     * started again. If it is not running (in a LATENT or STOPPED state) then
     * it will be restarted. If it is in a CLOSED state then an exception will
     * be thrown.
     */
    public void restart() throws Exception
    {
        // Can't restart from a closed state as all the temporary data is gone
        if ( state.get() == State.CLOSED )
        {
            throw new IllegalStateException("Cannot restart a closed instance");
        }

        // If the server's currently running then stop it.
        if ( state.get() == State.STARTED )
        {
            stop();
        }

        // Set to a LATENT state so we can restart
        state.set(State.LATENT);

        main = createServerMain();
        start();
    }

    public void stop() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.STOPPED) )
        {
            main.close();
        }
    }

    public InstanceSpec getInstanceSpec()
    {
        return configBuilder.getInstanceSpec(thisInstanceIndex);
    }

    @Override
    public void close() throws IOException
    {
        stop();

        if ( state.compareAndSet(State.STOPPED, State.CLOSED) )
        {
            configBuilder.close();

            InstanceSpec spec = getInstanceSpec();
            if ( spec.deleteDataDirectoryOnClose() )
            {
                DirectoryUtils.deleteRecursively(spec.getDataDirectory());
            }
        }
    }

    public void start() throws Exception
    {
        if ( !state.compareAndSet(State.LATENT, State.STARTED) )
        {
            return;
        }

        main.start(configBuilder.bindInstance(thisInstanceIndex, thisInstancePort));
        thisInstancePort = main.getClientPort();
    }

    public int getLocalPort() {
        if (thisInstancePort == 0) {
            throw new IllegalStateException("server is configured to bind to port 0 but not started");
        }
        return thisInstancePort;
    }
}