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

import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.jmx.ZKMBeanInfo;
import org.apache.zookeeper.server.ContainerManager;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import javax.management.JMException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TestingZooKeeperMain implements ZooKeeperMainFace
{
    private static final Logger log = LoggerFactory.getLogger(TestingZooKeeperMain.class);

    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicReference<Exception> startingException = new AtomicReference<Exception>(null);

    private volatile ServerCnxnFactory cnxnFactory;
    private volatile TestZooKeeperServer zkServer;
    private volatile ContainerManager containerManager;

    private static final Timing timing = new Timing();

    static final int MAX_WAIT_MS = timing.milliseconds();

    @Override
    public void kill()
    {
        try
        {
            if ( cnxnFactory != null )
            {
                cnxnFactory.closeAll();

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

    @Override
    public void runFromConfig(QuorumPeerConfig config) throws Exception
    {
        try
        {
            Field instance = MBeanRegistry.class.getDeclaredField("instance");
            instance.setAccessible(true);
            MBeanRegistry nopMBeanRegistry = new MBeanRegistry()
            {
                @Override
                public void register(ZKMBeanInfo bean, ZKMBeanInfo parent) throws JMException
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

    @Override
    public QuorumPeer getQuorumPeer()
    {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    @Override
    public void blockUntilStarted() throws Exception
    {
        Assert.assertTrue(timing.awaitLatch(latch));

        if ( zkServer != null )
        {
            //noinspection SynchronizeOnNonFinalField
            synchronized(zkServer)
            {
                while ( !zkServer.isRunning() )
                {
                    zkServer.wait();
                }
            }
        }
        else
        {
            throw new Exception("No zkServer.");
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
        FileTxnSnapLog txnLog = null;
        try {
            // Note that this thread isn't going to be doing anything else,
            // so rather than spawning another thread, we will just call
            // run() in this thread.
            // create a file logger url from the command line args
            txnLog = new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir());
            zkServer = new TestZooKeeperServer(txnLog, config);

            try
            {
                cnxnFactory = ServerCnxnFactory.createFactory();
                cnxnFactory.configure(config.getClientPortAddress(),
                    config.getMaxClientCnxns());
            }
            catch ( IOException e )
            {
                log.info("Could not server. Waiting and trying one more time.", e);
                timing.sleepABit();
                cnxnFactory = ServerCnxnFactory.createFactory();
                cnxnFactory.configure(config.getClientPortAddress(),
                    config.getMaxClientCnxns());
            }
            cnxnFactory.startup(zkServer);
            containerManager = new ContainerManager(zkServer.getZKDatabase(), zkServer.getFirstProcessor(), Integer.getInteger("znode.container.checkIntervalMs", (int)TimeUnit.MINUTES.toMillis(1L)).intValue(), Integer.getInteger("znode.container.maxPerMinute", 10000).intValue());
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
        } finally {
            if (txnLog != null) {
                txnLog.close();
            }
        }
    }

    public static class TestZooKeeperServer extends ZooKeeperServer
    {
        public TestZooKeeperServer(FileTxnSnapLog txnLog, ServerConfig config)
        {
            super(txnLog, config.getTickTime(), config.getMinSessionTimeout(), config.getMaxSessionTimeout(), null);
        }

        private final AtomicBoolean isRunning = new AtomicBoolean(false);

        public RequestProcessor getFirstProcessor()
        {
            return firstProcessor;
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
