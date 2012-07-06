package com.netflix.curator.test;

import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;

public class TestingZooKeeperMain extends ZooKeeperServerMain implements ZooKeeperMainFace
{
    private final CountDownLatch        latch = new CountDownLatch(1);

    private static final int MAX_WAIT_MS = 1000;

    @Override
    public void kill()
    {
        try
        {
            Field               cnxnFactoryField = ZooKeeperServerMain.class.getDeclaredField("cnxnFactory");
            cnxnFactoryField.setAccessible(true);
            NIOServerCnxn.Factory   cnxnFactory = (NIOServerCnxn.Factory)cnxnFactoryField.get(this);
            cnxnFactory.shutdown();

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
        ServerConfig        serverConfig = new ServerConfig();
        serverConfig.readFrom(config);
        latch.countDown();
        super.runFromConfig(serverConfig);
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    @Override
    public void blockUntilStarted() throws Exception
    {
        latch.await();

        NIOServerCnxn.Factory   cnxnFactory = getServerConnectionFactory();
        if ( cnxnFactory != null )
        {
            final ZooKeeperServer     zkServer = getZooKeeperServer(cnxnFactory);
            if ( zkServer != null )
            {
                synchronized ( zkServer )
                {
                    if ( !zkServer.isRunning() )
                    {
                        zkServer.wait();
                    }
                }
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        shutdown();

        try
        {
            NIOServerCnxn.Factory   cnxnFactory = getServerConnectionFactory();
            if ( cnxnFactory != null )
            {
                ZooKeeperServer     zkServer = getZooKeeperServer(cnxnFactory);
                if ( zkServer != null )
                {
                    ZKDatabase      zkDb = zkServer.getZKDatabase();
                    if ( zkDb != null )
                    {
                        // make ZK server close its log files
                        zkDb.close();
                    }
                }
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();    // just ignore - this class is only for testing
        }
    }

    private NIOServerCnxn.Factory getServerConnectionFactory() throws Exception
    {
        Field               cnxnFactoryField = ZooKeeperServerMain.class.getDeclaredField("cnxnFactory");
        cnxnFactoryField.setAccessible(true);
        NIOServerCnxn.Factory   cnxnFactory;

        // Wait until the cnxnFactory field is non-null or up to 1s, whichever comes first.
        long startTime = System.currentTimeMillis();
        do
        {
            cnxnFactory = (NIOServerCnxn.Factory)cnxnFactoryField.get(this);
        }
        while ( (cnxnFactory == null) && ((System.currentTimeMillis() - startTime) < MAX_WAIT_MS) );

        return cnxnFactory;
    }

    private ZooKeeperServer getZooKeeperServer(NIOServerCnxn.Factory cnxnFactory) throws Exception
    {
        Field               zkServerField = NIOServerCnxn.Factory.class.getDeclaredField("zks");
        zkServerField.setAccessible(true);
        ZooKeeperServer     zkServer;

        // Wait until the zkServer field is non-null or up to 1s, whichever comes first.
        long startTime = System.currentTimeMillis();
        do
        {
            zkServer = (ZooKeeperServer)zkServerField.get(cnxnFactory);
        }
        while ( (zkServer == null) && ((System.currentTimeMillis() - startTime) < MAX_WAIT_MS) );

        return zkServer;
    }
}
