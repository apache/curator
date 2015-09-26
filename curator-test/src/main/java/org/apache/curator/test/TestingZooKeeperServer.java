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

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thanks to Jeremie BORDIER (ahfeel) for this code
 */
public class TestingZooKeeperServer extends QuorumPeerMain implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(TestingZooKeeperServer.class);

    private final QuorumConfigBuilder configBuilder;
    private final int thisInstanceIndex;
    private volatile ZooKeeperMainFace main;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

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
        this.configBuilder = configBuilder;
        this.thisInstanceIndex = thisInstanceIndex;
        main = (configBuilder.size() > 1) ? new TestingQuorumPeerMain() : new TestingZooKeeperMain();
    }

    public QuorumPeer getQuorumPeer()
    {
        return main.getQuorumPeer();
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
     *
     * @throws Exception
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

        main = (configBuilder.size() > 1) ? new TestingQuorumPeerMain() : new TestingZooKeeperMain();
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

        new Thread(new Runnable()
        {
            public void run()
            {
                try
                {
                    QuorumPeerConfig config = configBuilder.buildConfig(thisInstanceIndex);
                    main.runFromConfig(config);
                }
                catch ( Exception e )
                {
                    logger.error(String.format("From testing server (random state: %s) for instance: %s", String.valueOf(configBuilder.isFromRandom()), getInstanceSpec()), e);
                }
            }
        }).start();

        main.blockUntilStarted();
    }
}