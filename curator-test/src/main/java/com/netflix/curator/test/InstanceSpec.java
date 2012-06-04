package com.netflix.curator.test;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstracts one of the servers in the ensemble
 */
public class InstanceSpec
{
    private static final AtomicInteger      nextServerId = new AtomicInteger(1);
    private static final String             localhost;
    static
    {
        String address = "localhost";
        try
        {
            // This is a workaround for people using OS X Lion.  On Lion when a process tries to connect to a link-local
            // address it takes 5 seconds to establish the connection for some reason.  So instead of using 'localhost'
            // which could return the link-local address randomly, we'll manually resolve it and look for an address to
            // return that isn't link-local.  If for some reason we can't find an address that isn't link-local then
            // we'll fall back to the default lof just looking up 'localhost'.
            for( InetAddress a : InetAddress.getAllByName("localhost") )
            {
              if ( !a.isLinkLocalAddress() )
              {
                address = a.getHostAddress();
                break;
              }
            }
        }
        catch ( UnknownHostException e )
        {
            // Something went wrong, just default to the existing approach of using 'localhost'.
        }
        localhost = address;
    }

    private final File      dataDirectory;
    private final int       port;
    private final int       electionPort;
    private final int       quorumPort;
    private final boolean   deleteDataDirectoryOnClose;
    private final int serverId;

    public static InstanceSpec      newInstanceSpec()
    {
        return new InstanceSpec(null, -1, -1, -1, true, -1);
    }

    public static int getRandomPort()
    {
        ServerSocket server = null;
        try
        {
            server = new ServerSocket(0);
            return server.getLocalPort();
        }
        catch ( IOException e )
        {
            throw new Error(e);
        }
        finally
        {
            if ( server != null )
            {
                try
                {
                    server.close();
                }
                catch ( IOException ignore )
                {
                    // ignore
                }
            }
        }
    }

    /**
     * @param dataDirectory where to store data/logs/etc.
     * @param port the port to listen on - each server in the ensemble must use a unique port
     * @param electionPort the electionPort to listen on - each server in the ensemble must use a unique electionPort
     * @param quorumPort the quorumPort to listen on - each server in the ensemble must use a unique quorumPort
     * @param deleteDataDirectoryOnClose if true, the data directory will be deleted when {@link TestingCluster#close()} is called
     * @param serverId the server ID for the instance
     */
    public InstanceSpec(File dataDirectory, int port, int electionPort, int quorumPort, boolean deleteDataDirectoryOnClose, int serverId)
    {
        this.dataDirectory = (dataDirectory != null) ? dataDirectory : Files.createTempDir();
        this.port = (port >= 0) ? port : getRandomPort();
        this.electionPort = (electionPort >= 0) ? electionPort : getRandomPort();
        this.quorumPort = (quorumPort >= 0) ? quorumPort : getRandomPort();
        this.deleteDataDirectoryOnClose = deleteDataDirectoryOnClose;
        this.serverId = (serverId >= 0) ? serverId : nextServerId.getAndIncrement();
    }

    public int getServerId()
    {
        return serverId;
    }

    public File getDataDirectory()
    {
        return dataDirectory;
    }

    public int getPort()
    {
        return port;
    }

    public int getElectionPort()
    {
        return electionPort;
    }

    public int getQuorumPort()
    {
        return quorumPort;
    }

    public String getConnectString()
    {
        return localhost + ":" + port;
    }

    public boolean deleteDataDirectoryOnClose()
    {
        return deleteDataDirectoryOnClose;
    }

    @Override
    public String toString()
    {
        return "InstanceSpec{" +
            "dataDirectory=" + dataDirectory +
            ", port=" + port +
            ", electionPort=" + electionPort +
            ", quorumPort=" + quorumPort +
            ", deleteDataDirectoryOnClose=" + deleteDataDirectoryOnClose +
            ", serverId=" + serverId +
            '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        InstanceSpec that = (InstanceSpec)o;

        return port == that.port;

    }

    @Override
    public int hashCode()
    {
        return port;
    }
}
