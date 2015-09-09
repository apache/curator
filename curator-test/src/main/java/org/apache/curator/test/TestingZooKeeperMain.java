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
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class TestingZooKeeperMain extends ZooKeeperServerMain implements ZooKeeperMainFace
{
    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicReference<Exception> startingException = new AtomicReference<Exception>(null);

    static final int MAX_WAIT_MS = new Timing().milliseconds();

    @Override
    public void kill()
    {
        try
        {
            Field cnxnFactoryField = ZooKeeperServerMain.class.getDeclaredField("cnxnFactory");
            cnxnFactoryField.setAccessible(true);
            ServerCnxnFactory cnxnFactory = (ServerCnxnFactory)cnxnFactoryField.get(this);
            cnxnFactory.closeAll();

            Field ssField = cnxnFactory.getClass().getDeclaredField("ss");
            ssField.setAccessible(true);
            ServerSocketChannel ss = (ServerSocketChannel)ssField.get(cnxnFactory);
            ss.close();

            close();
        }
        catch ( Exception e )
        {
            e.printStackTrace();    // just ignore - this class is only for testing
        }
    }

    @Override
    public void runFromConfig(QuorumPeerConfig config) throws Exception
    {
        ServerConfig serverConfig = new ServerConfig();
        serverConfig.readFrom(config);
        latch.countDown();
        try
        {
            super.runFromConfig(serverConfig);
        }
        catch ( IOException e )
        {
            startingException.set(e);
            throw e;
        }
    }

    @Override
    public QuorumPeer getQuorumPeer()
    {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    @Override
    public void blockUntilStarted() throws Exception
    {
        latch.await();

        ServerCnxnFactory cnxnFactory = getServerConnectionFactory();
        if ( cnxnFactory != null )
        {
            final ZooKeeperServer zkServer = getZooKeeperServer(cnxnFactory);
            if ( zkServer != null )
            {
                synchronized(zkServer)
                {
                    if ( !zkServer.isRunning() )
                    {
                        zkServer.wait();
                    }
                }
            }
            else
            {
                throw new Exception("No zkServer");
            }
        }
        else
        {
            throw new Exception("No connection factory");
        }

        Exception exception = startingException.get();
        if ( exception != null )
        {
            throw exception;
        }
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            shutdown();
        }
        catch ( Throwable e )
        {
            e.printStackTrace();    // just ignore - this class is only for testing
        }

        try
        {
            ServerCnxnFactory cnxnFactory = getServerConnectionFactory();
            if ( cnxnFactory != null )
            {
                ZooKeeperServer zkServer = getZooKeeperServer(cnxnFactory);
                if ( zkServer != null )
                {
                    ZKDatabase zkDb = zkServer.getZKDatabase();
                    if ( zkDb != null )
                    {
                        // make ZK server close its log files
                        zkDb.close();
                    }
                }
            }
        }
        catch ( Throwable e )
        {
            e.printStackTrace();    // just ignore - this class is only for testing
        }
    }

    private ServerCnxnFactory getServerConnectionFactory() throws Exception
    {
        Field cnxnFactoryField = ZooKeeperServerMain.class.getDeclaredField("cnxnFactory");
        cnxnFactoryField.setAccessible(true);
        ServerCnxnFactory cnxnFactory;

        // Wait until the cnxnFactory field is non-null or up to 1s, whichever comes first.
        long startTime = System.currentTimeMillis();
        do
        {
            cnxnFactory = (ServerCnxnFactory)cnxnFactoryField.get(this);
            if ( cnxnFactory == null )
            {
                try
                {
                    Thread.sleep(10);
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        while ( (cnxnFactory == null) && ((System.currentTimeMillis() - startTime) <= MAX_WAIT_MS) );

        return cnxnFactory;
    }

    private ZooKeeperServer getZooKeeperServer(ServerCnxnFactory cnxnFactory) throws Exception
    {
        Field zkServerField = ServerCnxnFactory.class.getDeclaredField("zkServer");
        zkServerField.setAccessible(true);
        ZooKeeperServer zkServer;

        // Wait until the zkServer field is non-null
        long startTime = System.currentTimeMillis();
        do
        {
            zkServer = (ZooKeeperServer)zkServerField.get(cnxnFactory);
            if ( zkServer == null )
            {
                try
                {
                    Thread.sleep(10);
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        while ( (zkServer == null) && ((System.currentTimeMillis() - startTime) < MAX_WAIT_MS) );

        return zkServer;
    }
}
