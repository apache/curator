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

package com.netflix.curator.ensemble.exhibitor;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.BaseClassForTests;
import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.RetryLoop;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.TestingServer;
import com.netflix.curator.test.Timing;
import junit.framework.Assert;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.Test;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

public class TestExhibitorEnsembleProvider extends BaseClassForTests
{
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
            Exhibitors                      exhibitors = new Exhibitors(Lists.newArrayList("foo", "bar"), 1000);
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
            Closeables.closeQuietly(secondServer);
        }
    }

    @Test
    public void     testSimple() throws Exception
    {
        Exhibitors                  exhibitors = new Exhibitors(Lists.newArrayList("foo", "bar"), 1000);
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
        CuratorZookeeperClient      client = new CuratorZookeeperClient(provider, timing.session(), timing.connection(), null, new RetryOneTime(2));
        client.start();
        try
        {
            client.blockUntilConnectedOrTimedOut();
            client.getZooKeeper().exists("/", false);
        }
        catch ( Exception e )
        {
            Assert.fail();
            throw e;
        }
        finally
        {
            client.close();
        }
    }
}
