/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator.utils;

import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * manages an internally running ZooKeeper server. FOR TESTING PURPOSES ONLY
 */
public class TestingServer
{
    private final ZooKeeperServer server;
    private final int port;
    private final NIOServerCnxn.Factory factory;
    private final File tempDirectory;

    private static final int TIME_IN_MS = 2000;

    private static int getRandomPort()
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
     * Create the server using a random port
     *
     * @throws Exception errors
     */
    public TestingServer() throws Exception
    {
        this(getRandomPort(), Files.createTempDir());
    }

    /**
     * Create the server using the given port
     *
     * @param port the port
     * @throws Exception errors
     */
    public TestingServer(int port) throws Exception
    {
        this(port, Files.createTempDir());
    }

    /**
     * Create the server using the given port
     *
     * @param port          the port
     * @param tempDirectory directory to use
     * @throws Exception errors
     */
    public TestingServer(int port, File tempDirectory) throws Exception
    {
        this.port = port;
        this.tempDirectory = tempDirectory;

        File logDir = new File(tempDirectory, "testLog");
        File dataDir = new File(tempDirectory, "testData");

        server = new ZooKeeperServer(dataDir, logDir, TIME_IN_MS);
        factory = new NIOServerCnxn.Factory(new InetSocketAddress(port));
        factory.startup(server);
    }

    /**
     * Return the port being used
     *
     * @return port
     */
    public int getPort()
    {
        return port;
    }

    /**
     * Returns the temp directory being used
     *
     * @return directory
     */
    public File getTempDirectory()
    {
        return tempDirectory;
    }

    /**
     * Stop the server without deleting the temp directory
     */
    public void stop()
    {
        server.shutdown();
        factory.shutdown();
    }

    /**
     * Close the server and any open clients and delete the temp directory
     */
    public void close()
    {
        try
        {
            stop();
        }
        finally
        {
            try
            {
                Files.deleteRecursively(tempDirectory);
            }
            catch ( IOException e )
            {
                // ignore
            }
        }
    }

    /**
     * Returns the connection string to use
     *
     * @return connection string
     */
    public String getConnectString()
    {
        return "localhost:" + port;
    }
}