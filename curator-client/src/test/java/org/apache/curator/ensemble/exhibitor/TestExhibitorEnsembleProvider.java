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
package org.apache.curator.ensemble.exhibitor;

import com.google.common.collect.Lists;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryLoop;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

public class TestExhibitorEnsembleProvider extends BaseClassForTests
{
    private static final Exhibitors.BackupConnectionStringProvider dummyConnectionStringProvider = new Exhibitors.BackupConnectionStringProvider()
    {
        @Override
        public String getBackupConnectionString() throws Exception
        {
            return null;
        }
    };

    @Test
    public void     testExhibitorFailures() throws Exception
    {
        final AtomicReference<String>   backupConnectionString = new AtomicReference<String>("backup1:1");
        final AtomicReference<String>   connectionString = new AtomicReference<String>("count=1&port=2&server0=localhost");
        Exhibitors                      exhibitors = new Exhibitors
        (
            Lists.newArrayList("foo", "bar"),
            1000,
            new Exhibitors.BackupConnectionStringProvider()
            {
                @Override
                public String getBackupConnectionString()
                {
                    return backupConnectionString.get();
                }
            }
        );
        ExhibitorRestClient             mockRestClient = new ExhibitorRestClient()
        {
            @Override
            public String getRaw(String hostname, int port, String uriPath, String mimeType) throws Exception
            {
                String localConnectionString = connectionString.get();
                if ( localConnectionString == null )
                {
                    throw new IOException();
                }
                return localConnectionString;
            }
        };

        final Semaphore             semaphore = new Semaphore(0);
        ExhibitorEnsembleProvider   provider = new ExhibitorEnsembleProvider(exhibitors, mockRestClient, "/foo", 10, new RetryOneTime(1))
        {
            @Override
            protected void poll()
            {
                super.poll();
                semaphore.release();
            }
        };
        provider.pollForInitialEnsemble();
        try
        {
            provider.start();

            Assert.assertEquals(provider.getConnectionString(), "localhost:2");

            connectionString.set(null);
            semaphore.drainPermits();
            semaphore.acquire();    // wait for next poll
            Assert.assertEquals(provider.getConnectionString(), "backup1:1");

            backupConnectionString.set("backup2:2");
            semaphore.drainPermits();
            semaphore.acquire();    // wait for next poll
            Assert.assertEquals(provider.getConnectionString(), "backup2:2");

            connectionString.set("count=1&port=3&server0=localhost3");
            semaphore.drainPermits();
            semaphore.acquire();    // wait for next poll
            Assert.assertEquals(provider.getConnectionString(), "localhost3:3");
        }
        finally
        {
            CloseableUtils.closeQuietly(provider);
        }
    }

    @Test
    public void     testChanging() throws Exception
    {
        TestingServer               secondServer = new TestingServer();
        try
        {
            String                          mainConnectionString = "count=1&port=" + server.getPort() + "&server0=localhost";
            String                          secondConnectionString = "count=1&port=" + secondServer.getPort() + "&server0=localhost";

            final Semaphore                 semaphore = new Semaphore(0);
            final AtomicReference<String>   connectionString = new AtomicReference<String>(mainConnectionString);
            Exhibitors                      exhibitors = new Exhibitors(Lists.newArrayList("foo", "bar"), 1000, dummyConnectionStringProvider);
            ExhibitorRestClient             mockRestClient = new ExhibitorRestClient()
            {
                @Override
                public String getRaw(String hostname, int port, String uriPath, String mimeType) throws Exception
                {
                    semaphore.release();
                    return connectionString.get();
                }
            };
            ExhibitorEnsembleProvider   provider = new ExhibitorEnsembleProvider(exhibitors, mockRestClient, "/foo", 10, new RetryOneTime(1));
            provider.pollForInitialEnsemble();

            Timing                          timing = new Timing().multiple(4);
            final CuratorZookeeperClient    client = new CuratorZookeeperClient(provider, timing.session(), timing.connection(), null, new RetryOneTime(2));
            client.start();
            try
            {
                RetryLoop.callWithRetry
                (
                    client,
                    new Callable<Object>()
                    {
                        @Override
                        public Object call() throws Exception
                        {
                            client.getZooKeeper().create("/test", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                            return null;
                        }
                    }
                );

                connectionString.set(secondConnectionString);
                semaphore.drainPermits();
                semaphore.acquire();

                server.stop();  // create situation where the current zookeeper gets a sys-disconnected

                Stat        stat = RetryLoop.callWithRetry
                (
                    client,
                    new Callable<Stat>()
                    {
                        @Override
                        public Stat call() throws Exception
                        {
                            return client.getZooKeeper().exists("/test", false);
                        }
                    }
                );
                Assert.assertNull(stat);    // it's a different server so should be null
            }
            finally
            {
                client.close();
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(secondServer);
        }
    }

    @Test
    public void     testSimple() throws Exception
    {
        Exhibitors                  exhibitors = new Exhibitors(Lists.newArrayList("foo", "bar"), 1000, dummyConnectionStringProvider);
        ExhibitorRestClient         mockRestClient = new ExhibitorRestClient()
        {
            @Override
            public String getRaw(String hostname, int port, String uriPath, String mimeType) throws Exception
            {
                return "count=1&port=" + server.getPort() + "&server0=localhost";
            }
        };
        ExhibitorEnsembleProvider   provider = new ExhibitorEnsembleProvider(exhibitors, mockRestClient, "/foo", 10, new RetryOneTime(1));
        provider.pollForInitialEnsemble();

        Timing                      timing = new Timing();
        CuratorZookeeperClient      client = new CuratorZookeeperClient(provider, timing.session(), timing.connection(), null, new ExponentialBackoffRetry(timing.milliseconds(), 3));
        client.start();
        try
        {
            client.blockUntilConnectedOrTimedOut();
            client.getZooKeeper().exists("/", false);
        }
        catch ( Exception e )
        {
            Assert.fail("provider.getConnectionString(): " + provider.getConnectionString() + " server.getPort(): " + server.getPort(), e);
        }
        finally
        {
            client.close();
        }
    }
}
