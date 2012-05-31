package com.netflix.curator.test;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;

public class TestingZooKeeperMain extends ZooKeeperServerMain implements ZooKeeperMainFace
{
    private final CountDownLatch        latch = new CountDownLatch(1);

    @Override
    public void kill()
    {
        try
        {
            Field               cnxnFactoryField = ZooKeeperServerMain.class.getDeclaredField("cnxnFactory");
            cnxnFactoryField.setAccessible(true);
            ServerCnxnFactory   cnxnFactory = (ServerCnxnFactory)cnxnFactoryField.get(this);
            cnxnFactory.closeAll();

            Field               ssField = cnxnFactory.getClass().getDeclaredField("ss");
            ssField.setAccessible(true);
            ServerSocketChannel ss = (ServerSocketChannel)ssField.get(cnxnFactory);
            ss.close();

            close();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
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

    @Override
    public void blockUntilStarted() throws Exception
    {
        latch.await();

        ServerCnxnFactory   cnxnFactory = getServerConnectionFactory();
        if ( cnxnFactory != null )
        {
            ZooKeeperServer     zkServer = getZooKeeperServer(cnxnFactory);
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
            ServerCnxnFactory   cnxnFactory = getServerConnectionFactory();
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
            e.printStackTrace();
        }
    }

    private ServerCnxnFactory getServerConnectionFactory() throws Exception
    {
        Field               cnxnFactoryField = ZooKeeperServerMain.class.getDeclaredField("cnxnFactory");
        cnxnFactoryField.setAccessible(true);
        ServerCnxnFactory   cnxnFactory;

        // Wait until the cnxnFactory field is non-null or up to 1s, whichever comes first.
        long startTime = System.currentTimeMillis();
        do
        {
            cnxnFactory = (ServerCnxnFactory)cnxnFactoryField.get(this);
        }
        while ( cnxnFactory == null && System.currentTimeMillis() - startTime < 1000 );

        return cnxnFactory;
    }

    private ZooKeeperServer getZooKeeperServer(ServerCnxnFactory cnxnFactory) throws Exception
    {
        Field               zkServerField = ServerCnxnFactory.class.getDeclaredField("zkServer");
        zkServerField.setAccessible(true);
        ZooKeeperServer     zkServer;

        // Wait until the zkServer field is non-null or up to 1s, whichever comes first.
        long startTime = System.currentTimeMillis();
        do
        {
            zkServer = (ZooKeeperServer)zkServerField.get(cnxnFactory);
        }
        while ( zkServer == null && System.currentTimeMillis() - startTime < 1000 );

        return zkServer;
    }
}
