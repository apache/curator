/*
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

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.jmx.ZKMBeanInfo;
import org.apache.zookeeper.server.ContainerManager;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestingZooKeeperMain implements ZooKeeperMainFace
{
    private static final Logger log = LoggerFactory.getLogger(TestingZooKeeperMain.class);

    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicReference<Exception> startingException = new AtomicReference<>(null);

    private volatile ServerCnxnFactory cnxnFactory;
    private volatile TestZooKeeperServer zkServer;
    private volatile ContainerManager containerManager;

    private static final Timing timing = new Timing();

    static final int MAX_WAIT_MS;
    static
    {
        long startMs = System.currentTimeMillis();
        try
        {
            // this can take forever and fails tests - ZK calls it internally so there's nothing we can do
            // pre flight it and use it to calculate max wait
            //noinspection ResultOfMethodCallIgnored
            InetAddress.getLocalHost().getCanonicalHostName();
        }
        catch ( UnknownHostException e )
        {
            // ignore
        }
        long elapsed = System.currentTimeMillis() - startMs;
        MAX_WAIT_MS = Math.max((int)elapsed * 2, 1000);
    }

    @Override
    public void kill()
    {
        try
        {
            if ( cnxnFactory != null )
            {
                Compatibility.serverCnxnFactoryCloseAll(cnxnFactory);

                Field ssField = cnxnFactory.getClass().getDeclaredField("ss");
                ssField.setAccessible(true);
                ServerSocketChannel ss = (ServerSocketChannel)ssField.get(cnxnFactory);
                ss.close();
            }

            close();
        }
        catch ( Exception e )
        {
            e.printStackTrace();    // just ignore - this class is only for testing
        }
    }

    TestZooKeeperServer getZkServer() {
        return zkServer;
    }

    private void runFromConfig(QuorumPeerConfig config) throws Exception
    {
        try
        {
            Field instance = MBeanRegistry.class.getDeclaredField("instance");
            instance.setAccessible(true);
            MBeanRegistry nopMBeanRegistry = new MBeanRegistry()
            {
                @Override
                public void register(ZKMBeanInfo bean, ZKMBeanInfo parent)
                {
                    // NOP
                }

                @Override
                public void unregister(ZKMBeanInfo bean)
                {
                    // NOP
                }
            };
            instance.set(null, nopMBeanRegistry);
        }
        catch ( Exception e )
        {
            log.error("Could not fix MBeanRegistry");
        }

        ServerConfig serverConfig = new ServerConfig();
        serverConfig.readFrom(config);
        try
        {
            internalRunFromConfig(serverConfig);
        }
        catch ( IOException e )
        {
            startingException.set(e);
            throw e;
        }
    }

    private void blockUntilStarted()
    {
        if (!timing.awaitLatch(latch))
        {
            throw new FailedServerStartException("Timed out waiting for server startup");
        }

        if ( zkServer != null )
        {
            //noinspection SynchronizeOnNonFinalField
            synchronized(zkServer)
            {
                while ( !zkServer.isRunning() )
                {
                    try
                    {
                        zkServer.wait();
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                        throw new FailedServerStartException("Server start interrupted");
                    }
                }
            }
        }
        else
        {
            throw new FailedServerStartException("No zkServer.");
        }

        Exception exception = startingException.get();
        if ( exception != null )
        {
            throw new FailedServerStartException(exception);
        }
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            cnxnFactory.shutdown();
        }
        catch ( Throwable e )
        {
            e.printStackTrace();    // just ignore - this class is only for testing
        }
        finally
        {
            cnxnFactory = null;
        }

        if ( containerManager != null ) {
            containerManager.stop();
            containerManager = null;
        }

        try
        {
            if ( zkServer != null )
            {
                zkServer.shutdown();
                ZKDatabase zkDb = zkServer.getZKDatabase();
                if ( zkDb != null )
                {
                    // make ZK server close its log files
                    zkDb.close();
                }
            }
        }
        catch ( Throwable e )
        {
            e.printStackTrace();    // just ignore - this class is only for testing
        }
        finally
        {
            zkServer = null;
        }
    }

    // copied from ZooKeeperServerMain.java
    private void internalRunFromConfig(ServerConfig config) throws IOException
    {
        log.info("Starting server");
        try {
            // Note that this thread isn't going to be doing anything else,
            // so rather than spawning another thread, we will just call
            // run() in this thread.
            // create a file logger url from the command line args
            FileTxnSnapLog txnLog = new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir());
            zkServer = new TestZooKeeperServer(txnLog, config);

            try
            {
                cnxnFactory = ServerCnxnFactory.createFactory();
                cnxnFactory.configure(config.getClientPortAddress(),
                    config.getMaxClientCnxns());
            }
            catch ( IOException e )
            {
                log.info("Could not start server. Waiting and trying one more time.", e);
                timing.sleepABit();
                cnxnFactory = ServerCnxnFactory.createFactory();
                cnxnFactory.configure(config.getClientPortAddress(),
                    config.getMaxClientCnxns());
            }
            cnxnFactory.startup(zkServer);
            containerManager = new ContainerManager(zkServer.getZKDatabase(), zkServer.getFirstProcessor(), Integer.getInteger("znode.container.checkIntervalMs", (int)TimeUnit.MINUTES.toMillis(1L)), Integer.getInteger("znode.container.maxPerMinute", 10000));
            containerManager.start();
            latch.countDown();
            cnxnFactory.join();
            if ( (zkServer != null) && zkServer.isRunning()) {
                zkServer.shutdown();
            }
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            Thread.currentThread().interrupt();
            log.warn("Server interrupted", e);
        }
    }

    @Override
    public void start(QuorumPeerConfigBuilder configBuilder) {
        new Thread(() -> {
            try
            {
                runFromConfig(configBuilder.buildConfig());
            }
            catch ( Exception e )
            {
                log.error(String.format("From testing server (random state: %s) for instance: %s", configBuilder.isFromRandom(), configBuilder.getInstanceSpec()), e);
            }
        }, "zk-main-thread").start();

        blockUntilStarted();
    }

    @Override
    public int getClientPort() {
        return cnxnFactory == null ? -1 : cnxnFactory.getLocalPort();
    }

    public static class TestZooKeeperServer extends ZooKeeperServer
    {
        private final FileTxnSnapLog txnLog;

        public TestZooKeeperServer(FileTxnSnapLog txnLog, ServerConfig config)
        {
            this.txnLog = txnLog;
            this.setTxnLogFactory(txnLog);
            // tickTime would affect min and max session timeout: should be set first
            this.setTickTime(config.getTickTime());
            this.setMinSessionTimeout(config.getMinSessionTimeout());
            this.setMaxSessionTimeout(config.getMaxSessionTimeout());
        }

        @Override
        public synchronized void shutdown(boolean fullyShutDown)
        {
            super.shutdown(fullyShutDown);
            try
            {
                txnLog.close();
            }
            catch ( IOException e )
            {
                // ignore
            }
        }

        private final AtomicBoolean isRunning = new AtomicBoolean(false);

        public RequestProcessor getFirstProcessor()
        {
            return firstProcessor;
        }

        @Override
        protected void setState(State state)
        {
            this.state = state;
            // avoid ZKShutdownHandler is not registered message
        }

        protected void registerJMX()
        {
            // NOP
        }

        @Override
        protected void unregisterJMX()
        {
            // NOP
        }

        @Override
        public boolean isRunning()
        {
            return isRunning.get() || super.isRunning();
        }

        public void noteStartup()
        {
            synchronized (this) {
                isRunning.set(true);
                this.notifyAll();
            }
        }
    }
}
