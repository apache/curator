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

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.ServerSocketChannel;

class TestingQuorumPeerMain extends QuorumPeerMain implements ZooKeeperMainFace
{
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
                cnxnFactory.closeAll();

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

    public QuorumPeer getTestingQuorumPeer()
    {
        return quorumPeer;
    }

    @Override
    public void close() throws IOException
    {
        if ( quorumPeer != null )
        {
            quorumPeer.shutdown();
        }
    }

    @Override
    public void blockUntilStarted() throws Exception
    {
        while ( quorumPeer == null )
        {
            try
            {
                Thread.sleep(100);
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
