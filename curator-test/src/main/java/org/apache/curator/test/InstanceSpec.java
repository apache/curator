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

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstracts one of the servers in the ensemble
 */
public class InstanceSpec
{
    private static final AtomicInteger nextServerId = new AtomicInteger(1);
    private static final String localhost;

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
            for ( InetAddress a : InetAddress.getAllByName("localhost") )
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

    private final File dataDirectory;
    private int port;
    private int electionPort;
    private int quorumPort;
    private final boolean deleteDataDirectoryOnClose;
    private final int serverId;
    private final int tickTime;
    private final int maxClientCnxns;
    private final Map<String,Object> customProperties;
    private final String hostname;

    public static void reset() {
        nextServerId.set(1);
    }

    public static InstanceSpec newInstanceSpec()
    {
        return new InstanceSpec(null, -1, -1, -1, true, -1, -1, -1);
    }

    public static int getRandomPort()
    {
        ServerSocket server = null;
        try
        {
            server = new ServerSocket(0);
            server.setReuseAddress(true);
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
     * @param dataDirectory              where to store data/logs/etc.
     * @param port                       the port to listen on - each server in the ensemble must use a unique port
     * @param electionPort               the electionPort to listen on - each server in the ensemble must use a unique electionPort
     * @param quorumPort                 the quorumPort to listen on - each server in the ensemble must use a unique quorumPort
     * @param deleteDataDirectoryOnClose if true, the data directory will be deleted when {@link TestingCluster#close()} is called
     * @param serverId                   the server ID for the instance
     */
    public InstanceSpec(File dataDirectory, int port, int electionPort, int quorumPort, boolean deleteDataDirectoryOnClose, int serverId)
    {
        this(dataDirectory, port, electionPort, quorumPort, deleteDataDirectoryOnClose, serverId, -1, -1, null, null);
    }

    /**
     * @param dataDirectory              where to store data/logs/etc.
     * @param port                       the port to listen on - each server in the ensemble must use a unique port
     * @param electionPort               the electionPort to listen on - each server in the ensemble must use a unique electionPort
     * @param quorumPort                 the quorumPort to listen on - each server in the ensemble must use a unique quorumPort
     * @param deleteDataDirectoryOnClose if true, the data directory will be deleted when {@link TestingCluster#close()} is called
     * @param serverId                   the server ID for the instance
     * @param tickTime                   tickTime. Set -1 to used fault server configuration
     * @param maxClientCnxns             max number of client connections from the same IP. Set -1 to use default server configuration
     */
    public InstanceSpec(File dataDirectory, int port, int electionPort, int quorumPort, boolean deleteDataDirectoryOnClose, int serverId, int tickTime, int maxClientCnxns) {
        this(dataDirectory, port, electionPort, quorumPort, deleteDataDirectoryOnClose, serverId, tickTime, maxClientCnxns, null, null);
    }

    /**
     * @param dataDirectory              where to store data/logs/etc.
     * @param port                       the port to listen on - each server in the ensemble must use a unique port
     * @param electionPort               the electionPort to listen on - each server in the ensemble must use a unique electionPort
     * @param quorumPort                 the quorumPort to listen on - each server in the ensemble must use a unique quorumPort
     * @param deleteDataDirectoryOnClose if true, the data directory will be deleted when {@link TestingCluster#close()} is called
     * @param serverId                   the server ID for the instance
     * @param tickTime                   tickTime. Set -1 to used fault server configuration
     * @param maxClientCnxns             max number of client connections from the same IP. Set -1 to use default server configuration
     * @param customProperties           other properties to be passed to the server
     */
    public InstanceSpec(File dataDirectory, int port, int electionPort, int quorumPort, boolean deleteDataDirectoryOnClose, int serverId, int tickTime, int maxClientCnxns, Map<String,Object> customProperties)
    {
        this(dataDirectory, port, electionPort, quorumPort, deleteDataDirectoryOnClose, serverId, tickTime, maxClientCnxns, customProperties, null);
    }

    /**
     * @param dataDirectory              where to store data/logs/etc.
     * @param port                       the port to listen on - each server in the ensemble must use a unique port
     * @param electionPort               the electionPort to listen on - each server in the ensemble must use a unique electionPort
     * @param quorumPort                 the quorumPort to listen on - each server in the ensemble must use a unique quorumPort
     * @param deleteDataDirectoryOnClose if true, the data directory will be deleted when {@link TestingCluster#close()} is called
     * @param serverId                   the server ID for the instance
     * @param tickTime                   tickTime. Set -1 to used fault server configuration
     * @param maxClientCnxns             max number of client connections from the same IP. Set -1 to use default server configuration
     * @param customProperties           other properties to be passed to the server
     * @param hostname                   Hostname or IP if the cluster is intending to be bounded into external interfaces
     */
    public InstanceSpec(File dataDirectory, int port, int electionPort, int quorumPort, boolean deleteDataDirectoryOnClose, int serverId, int tickTime, int maxClientCnxns, Map<String,Object> customProperties,String hostname)
    {
        this.dataDirectory = (dataDirectory != null) ? dataDirectory : Files.createTempDir();
        this.port = port;
        this.electionPort = port;
        this.quorumPort = port;
        this.deleteDataDirectoryOnClose = deleteDataDirectoryOnClose;
        this.serverId = (serverId >= 0) ? serverId : nextServerId.getAndIncrement();
        this.tickTime = (tickTime > 0 ? tickTime : -1); // -1 to set default value
        this.maxClientCnxns = (maxClientCnxns >= 0 ? maxClientCnxns : -1); // -1 to set default value
        this.customProperties = customProperties != null ? Collections.<String,Object>unmodifiableMap(customProperties) : Collections.<String,Object>emptyMap();
        this.hostname = hostname == null ? localhost : hostname;
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
        if (port < 0) {
            port = getRandomPort();
        }
        return port;
    }

    public int getElectionPort()
    {
        if (electionPort < 0) {
            electionPort = getRandomPort();
        }
        return electionPort;
    }

    public int getQuorumPort()
    {
        if (quorumPort < 0) {
            quorumPort = getRandomPort();
        }
        return quorumPort;
    }

    public String getConnectString()
    {
        return hostname + ":" + port;
    }

    public int getTickTime()
    {
        return tickTime;
    }

    public int getMaxClientCnxns()
    {
        return maxClientCnxns;
    }

    public boolean deleteDataDirectoryOnClose()
    {
        return deleteDataDirectoryOnClose;
    }

    public Map<String, Object> getCustomProperties() {
        return customProperties;
    }

    public String getHostname() {
        return hostname;
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
            ", tickTime=" + tickTime +
            ", maxClientCnxns=" + maxClientCnxns +
            ", customProperties=" + customProperties +
            ", hostname=" + hostname +
            "} " + super.toString();
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

        return hostname.equals(that.getHostname()) && port >= 0 && port == that.port;

    }

    @Override
    public int hashCode()
    {
        return hostname.hashCode() + port;
    }
}
