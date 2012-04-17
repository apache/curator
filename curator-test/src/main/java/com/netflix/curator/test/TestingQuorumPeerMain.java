package com.netflix.curator.test;

import org.apache.zookeeper.server.NIOServerCnxn;
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
                Field                   cnxnFactoryField = QuorumPeer.class.getDeclaredField("cnxnFactory");
                cnxnFactoryField.setAccessible(true);
                NIOServerCnxn.Factory   cnxnFactory = (NIOServerCnxn.Factory)cnxnFactoryField.get(quorumPeer);
                cnxnFactory.shutdown();

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
