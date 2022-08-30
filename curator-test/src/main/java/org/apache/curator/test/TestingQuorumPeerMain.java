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

import java.lang.reflect.Field;
import java.nio.channels.ServerSocketChannel;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestingQuorumPeerMain extends QuorumPeerMain implements ZooKeeperMainFace
{
    private static final Logger log = LoggerFactory.getLogger(TestingQuorumPeerMain.class);
    private volatile boolean isClosed = false;

    private volatile QuorumConfigBuilder configBuilder;
    private volatile int instanceIndex;

    @Override
    public void kill()
    {
        try
        {
            if ( quorumPeer != null )
            {
                Field               cnxnFactoryField = QuorumPeer.class.getDeclaredField("cnxnFactory");
                cnxnFactoryField.setAccessible(true);
                ServerCnxnFactory   cnxnFactory = (ServerCnxnFactory)cnxnFactoryField.get(quorumPeer);
                Compatibility.serverCnxnFactoryCloseAll(cnxnFactory);

                Field               ssField = cnxnFactory.getClass().getDeclaredField("ss");
                ssField.setAccessible(true);
                ServerSocketChannel ss = (ServerSocketChannel)ssField.get(cnxnFactory);
                ss.close();
            }
            close();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    @Override
    public void close()
    {
        if ( (quorumPeer != null) && !isClosed )
        {
            isClosed = true;
            quorumPeer.shutdown();
        }
    }

    private void blockUntilStarted()
    {
        long startTime = System.currentTimeMillis();
        while ( (quorumPeer == null) && ((System.currentTimeMillis() - startTime) <= TestingZooKeeperMain.MAX_WAIT_MS) )
        {
            try
            {
                Thread.sleep(10);
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if ( quorumPeer == null )
        {
            throw new FailedServerStartException("quorumPeer never got set");
        }
    }

    @Override
    public void configure(QuorumConfigBuilder configBuilder, int instanceIndex) {
        this.configBuilder = configBuilder;
        this.instanceIndex = instanceIndex;
    }

    @Override
    public void start() {
        new Thread(() -> {
            try {
                QuorumPeerConfig config = configBuilder.buildConfig(instanceIndex);
                runFromConfig(config);
            } catch (Exception e) {
                log.error("From testing server (random state: {}) for instance: {}", configBuilder.isFromRandom(), configBuilder.getInstanceSpec(instanceIndex), e);
            }
        }).start();

        blockUntilStarted();
    }
}
