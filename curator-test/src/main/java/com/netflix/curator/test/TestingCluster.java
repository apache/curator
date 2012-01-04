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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * manages an internally running ensemble of ZooKeeper servers. FOR TESTING PURPOSES ONLY
 */
public class TestingCluster implements Closeable
{
    private final List<QuorumPeerEntry>     entries;
    private final ExecutorService           executorService;

    static
    {
        /*
            This ugliness is necessary. There is no way to tell ZK to not register JMX beans. Something
            in the shutdown of a QuorumPeer causes the state of the MBeanRegistry to get confused and
            generates an assert Exception.
         */
        ClassPool pool = ClassPool.getDefault();
        try
        {
            CtClass cc = pool.get("org.apache.zookeeper.server.quorum.LearnerZooKeeperServer");
            pool.appendClassPath(new javassist.LoaderClassPath(TestingCluster.class.getClassLoader()));     // re: https://github.com/Netflix/curator/issues/11
            for ( CtMethod method : cc.getMethods() )
            {
                if ( method.getName().equals("registerJMX") || method.getName().equals("unregisterJMX") )
                {
                    method.setBody(null);
                }
            }
            cc.toClass();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    /**
     * Abstracts one of the servers in the ensemble
     */
    public static class InstanceSpec
    {
        private final File      dataDirectory;
        private final int       port;
        private final int       electionPort;
        private final int       quorumPort;
        private final boolean   deleteDataDirectoryOnClose;

        /**
         * @param dataDirectory where to store data/logs/etc.
         * @param port the port to listen on - each server in the ensemble must use a unique port
         * @param electionPort the electionPort to listen on - each server in the ensemble must use a unique electionPort
         * @param quorumPort the quorumPort to listen on - each server in the ensemble must use a unique quorumPort
         * @param deleteDataDirectoryOnClose if true, the data directory will be deleted when {@link TestingCluster#close()} is called
         */
        public InstanceSpec(File dataDirectory, int port, int electionPort, int quorumPort, boolean deleteDataDirectoryOnClose)
        {
            this.dataDirectory = dataDirectory;
            this.port = port;
            this.electionPort = electionPort;
            this.quorumPort = quorumPort;
            this.deleteDataDirectoryOnClose = deleteDataDirectoryOnClose;
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

        @Override
        public String toString()
        {
            return "Port: " + port + ", " +
                "electionPort: " + electionPort + ", " +
                "quorumPort: " + quorumPort + ", " +
                "dataDirectory: " + dataDirectory
                ;
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

    private static class QuorumPeerEntry
    {
        private volatile QuorumPeer       quorumPeer;
        private final InstanceSpec        instanceSpec;

        private QuorumPeerEntry(InstanceSpec instanceSpec)
        {
            this.instanceSpec = instanceSpec;
        }
    }

    /**
     * Creates an ensemble comprised of <code>n</code> servers. Each server will use
     * a temp directory and random ports
     *
     * @param instanceQty number of servers to create in the ensemble
     */
    public TestingCluster(int instanceQty)
    {
        this(makeSpecs(instanceQty));
    }

    /**
     * Creates an ensemble using the given server specs
     *
     * @param specs the server specs
     */
    public TestingCluster(InstanceSpec... specs)
    {
        ImmutableList.Builder<QuorumPeerEntry>      builder = ImmutableList.builder();
        for ( InstanceSpec spec : specs )
        {
            builder.add(new QuorumPeerEntry(spec));
        }

        entries = builder.build();
        executorService = Executors.newCachedThreadPool();
    }

    /**
     * Returns the set of servers in the ensemble
     *
     * @return set of servers
     */
    public Collection<InstanceSpec> getInstances()
    {
        Iterable<InstanceSpec> transformed = Iterables.transform
        (
            entries,
            new Function<QuorumPeerEntry, InstanceSpec>()
            {
                @Override
                public InstanceSpec apply(QuorumPeerEntry entry)
                {
                    return entry.instanceSpec;
                }
            }
        );
        return ImmutableList.copyOf(transformed);
    }

    /**
     * Returns the connection string to pass to the ZooKeeper constructor
     *
     * @return connection string
     */
    public String   getConnectString()
    {
        String              comma = "";
        StringBuilder       str = new StringBuilder();
        for ( QuorumPeerEntry entry : entries )
        {
            str.append(comma).append("localhost:").append(entry.instanceSpec.port);
            comma = ",";
        }
        return str.toString();
    }

    /**
     * Start the ensemble. The cluster must be started before use.
     *
     * @throws Exception errors
     */
    public void     start() throws Exception
    {
        final Map<QuorumPeerEntry, QuorumPeer.QuorumServer> serverMap = Maps.newHashMap();
        final Map<Long, QuorumPeer.QuorumServer>            testingServers = Maps.newHashMap();
        for ( final QuorumPeerEntry entry : entries )
        {
            InetSocketAddress       address = new InetSocketAddress("localhost", entry.instanceSpec.quorumPort);
            InetSocketAddress       electionAddress = new InetSocketAddress("localhost", entry.instanceSpec.electionPort);
            long                    serverId = serverMap.size() + 1;
            QuorumPeer.QuorumServer quorumServer = new QuorumPeer.QuorumServer(serverId, address, electionAddress);
            serverMap.put(entry, quorumServer);
            testingServers.put(serverId, quorumServer);
        }

        final CountDownLatch        startupLatch = new CountDownLatch(entries.size());
        final QuorumVerifier        testingQuorumVerifier = new QuorumMaj(testingServers.size());
        for ( final QuorumPeerEntry entry : entries )
        {
            final long                  thisServerId = serverMap.get(entry).id;
            final String                path = entry.instanceSpec.dataDirectory.getCanonicalPath();
            final InetSocketAddress     clientPort = new InetSocketAddress(entry.instanceSpec.port);
            final QuorumPeerConfig      config = new QuorumPeerConfig()
            {
                @Override
                public String getDataDir()
                {
                    return path;
                }

                @Override
                public QuorumVerifier getQuorumVerifier()
                {
                    return testingQuorumVerifier;
                }

                @Override
                public InetSocketAddress getClientPortAddress()
                {
                    return clientPort;
                }

                @Override
                public int getElectionPort()
                {
                    return entry.instanceSpec.electionPort;
                }

                @Override
                public String getDataLogDir()
                {
                    return path;
                }

                @Override
                public int getTickTime()
                {
                    return 2000;
                }

                @Override
                public long getServerId()
                {
                    return thisServerId;
                }

                @Override
                public Map<Long, QuorumPeer.QuorumServer> getServers()
                {
                    return testingServers;
                }

                @Override
                public int getSyncLimit()
                {
                    return 5;
                }

                @Override
                public int getInitLimit()
                {
                    return 10;
                }
            };

            executorService.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        try
                        {
                            Object      factory = ServerHelper.makeFactory(null, config.getClientPortAddress().getPort());

                            // copied from QuorumPeerMain.runFromConfig
                            entry.quorumPeer = new QuorumPeer();
                            entry.quorumPeer.setClientPortAddress(config.getClientPortAddress());
                            entry.quorumPeer.setTxnFactory(new FileTxnSnapLog(
                                new File(config.getDataLogDir()),
                                new File(config.getDataDir())));
                            entry.quorumPeer.setQuorumPeers(config.getServers());
                            entry.quorumPeer.setElectionType(config.getElectionAlg());
                            entry.quorumPeer.setMyid(config.getServerId());
                            entry.quorumPeer.setTickTime(config.getTickTime());
                            entry.quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
                            entry.quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
                            entry.quorumPeer.setInitLimit(config.getInitLimit());
                            entry.quorumPeer.setSyncLimit(config.getSyncLimit());
                            entry.quorumPeer.setQuorumVerifier(config.getQuorumVerifier());
                            entry.quorumPeer.setZKDatabase(new ZKDatabase(entry.quorumPeer.getTxnFactory()));
                            entry.quorumPeer.setLearnerType(config.getPeerType());

                            for ( Method m : QuorumPeer.class.getMethods() )
                            {
                                if ( m.getName().equals("setCnxnFactory") )
                                {
                                    // same as entry.quorumPeer.setCnxnFactory(factory);
                                    m.invoke(entry.quorumPeer, factory);
                                    break;
                                }
                            }

                            entry.quorumPeer.start();

                            startupLatch.countDown();
                            entry.quorumPeer.join();
                        }
                        catch ( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                        }
                        catch ( Throwable e )
                        {
                            e.printStackTrace();
                            throw new IOException(e);
                        }
                        return null;
                    }
                }
            );
        }

        if ( !startupLatch.await(5, TimeUnit.SECONDS) )
        {
            throw new Exception("Servers didn't start");
        }
    }

    /**
     * Shutdown the ensemble, free resources, etc. If temp directories were used, they
     * are deleted. You should call this in a <code>finally</code> block.
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException
    {
        for ( QuorumPeerEntry entry : entries )
        {
            closeEntry(entry);
        }

        executorService.shutdownNow();
    }

    /**
     * Kills the given server. This simulates the server unexpectedly crashing
     *
     * @param instance server to kill
     * @throws Exception errors
     * @return true if the instance was found
     */
    public boolean killServer(InstanceSpec instance) throws Exception
    {
        boolean     found = false;
        for ( QuorumPeerEntry entry : entries )
        {
            if ( entry.instanceSpec.port == instance.port )
            {
                closeEntry(entry);
                found = true;
                break;
            }
        }

        return found;
    }

    /**
     * Given a ZooKeeper instance, returns which server it is connected to
     *
     * @param client ZK instance
     * @return the server
     * @throws Exception errors
     */
    public InstanceSpec findConnectionInstance(ZooKeeper client) throws Exception
    {
        Method              m = client.getClass().getDeclaredMethod("testableRemoteSocketAddress");
        m.setAccessible(true);
        InetSocketAddress   address = (InetSocketAddress)m.invoke(client);
        for ( QuorumPeerEntry entry : entries )
        {
            if ( entry.instanceSpec.port == address.getPort() )
            {
                return entry.instanceSpec;
            }
        }
        return null;
    }

    private void closeEntry(QuorumPeerEntry entry)
    {
        entry.quorumPeer.shutdown();
        try
        {
            if ( entry.instanceSpec.deleteDataDirectoryOnClose )
            {
                DirectoryUtils.deleteRecursively(entry.instanceSpec.dataDirectory);
            }
        }
        catch ( IOException e )
        {
            // ignore
        }
    }

    private static InstanceSpec[] makeSpecs(int instanceQty)
    {
        InstanceSpec[]      specs = new InstanceSpec[instanceQty];
        for ( int i = 0; i < instanceQty; ++i )
        {
            specs[i] = new InstanceSpec(Files.createTempDir(), TestingServer.getRandomPort(), TestingServer.getRandomPort(), TestingServer.getRandomPort(), true);
        }
        return specs;
    }
}
