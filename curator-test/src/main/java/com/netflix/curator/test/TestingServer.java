/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.curator.test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

/**
 * manages an internally running ZooKeeper server. FOR TESTING PURPOSES ONLY
 */
public class TestingServer implements Closeable
{
    private final TestingZooKeeperServer testingZooKeeperServer;
    private final InstanceSpec spec;

    static int getRandomPort()
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
        this(-1, null);
    }

    /**
     * Create the server using the given port
     *
     * @param port the port
     * @throws Exception errors
     */
    public TestingServer(int port) throws Exception
    {
        this(port, null);
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
        this(new InstanceSpec(tempDirectory, port, -1, -1, true, -1));
    }

    public TestingServer(InstanceSpec spec) throws Exception
    {
        this.spec = spec;
        testingZooKeeperServer = new TestingZooKeeperServer(new QuorumConfigBuilder(spec));
        testingZooKeeperServer.start();
    }

    /**
     * Return the port being used
     *
     * @return port
     */
    public int getPort()
    {
        return spec.getPort();
    }

    /**
     * Returns the temp directory being used
     *
     * @return directory
     */
    public File getTempDirectory()
    {
        return spec.getDataDirectory();
    }

    /**
     * Stop the server without deleting the temp directory
     */
    public void stop() throws IOException
    {
        testingZooKeeperServer.stop();
    }

    /**
     * Close the server and any open clients and delete the temp directory
     */
    @Override
    public void close() throws IOException
    {
        testingZooKeeperServer.close();
    }

    /**
     * Returns the connection string to use
     *
     * @return connection string
     */
    public String getConnectString()
    {
        return spec.getConnectString();
    }
}