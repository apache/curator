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

package org.apache.curator.x.rest.api;

import ch.qos.logback.core.util.CloseUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.Timing;
import org.apache.curator.x.rest.entities.CreateSpec;
import org.apache.curator.x.rest.entities.ExistsSpec;
import org.apache.curator.x.rest.entities.PathAndId;
import org.apache.curator.x.rest.entities.Status;
import org.apache.curator.x.rest.entities.StatusMessage;
import org.apache.curator.x.rest.support.BaseClassForTests;
import org.apache.curator.x.rest.support.SessionManager;
import org.apache.curator.x.rest.support.StatusListener;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.Test;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class TestClient extends BaseClassForTests
{
    @Test
    public void testStatus() throws Exception
    {
        boolean connected = false;
        for ( int i = 0; i < 10; ++i )
        {
            Status status = restClient.resource(getStatusUri()).get(Status.class);
            if ( status.getState().equals("connected") )
            {
                connected = true;
                break;
            }
            Thread.sleep(1000);
        }
        Assert.assertTrue(connected);
    }

    @Test
    public void testSession() throws Exception
    {
        final String path = "/my/path";

        CuratorFramework client = null;
        SessionManager sessionManager = new SessionManager(restClient, curatorConfiguration.getSessionLengthMs());
        try
        {
            CreateSpec createSpec = new CreateSpec();
            createSpec.setPath(path);
            createSpec.setCreatingParentsIfNeeded(true);
            createSpec.setMode(CreateMode.EPHEMERAL);
            PathAndId pathAndId = restClient.resource(getMethodUri("create")).type(MediaType.APPLICATION_JSON).post(PathAndId.class, createSpec);

            final AtomicReference<String> expiredId = new AtomicReference<String>();
            StatusListener listener = new StatusListener()
            {
                @Override
                public void statusUpdate(List<StatusMessage> messages)
                {
                    for ( StatusMessage statusMessage : messages )
                    {
                        if ( statusMessage.getType().equals("expired") )
                        {
                            if ( expiredId.get() != null )
                            {
                                expiredId.set("-1");
                            }
                            else
                            {
                                expiredId.set(statusMessage.getSourceId());
                            }
                        }
                    }
                }

                @Override
                public void errorState(Status status)
                {
                }
            };
            sessionManager.addEntry(new InetSocketAddress("localhost", PORT), pathAndId.getId(), listener);

            Thread.sleep(2 * curatorConfiguration.getSessionLengthMs());

            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
            client.start();
            Assert.assertNotNull(client.checkExists().forPath(path));

            sessionManager.removeEntry(new InetSocketAddress("localhost", PORT), pathAndId.getId());
            Thread.sleep(2 * curatorConfiguration.getSessionLengthMs());
            Assert.assertNull(client.checkExists().forPath(path));
            Assert.assertEquals(pathAndId.getId(), expiredId.get());
        }
        finally
        {
            CloseUtil.closeQuietly(sessionManager);
            CloseUtil.closeQuietly(client);
        }
    }

    @Test
    public void testWatcher() throws Exception
    {
        final String path = "/a/path/to/a/node";
        final String watchId = "test-watcher";

        ExistsSpec existsSpec = new ExistsSpec();
        existsSpec.setPath(path);
        existsSpec.setWatched(true);
        existsSpec.setWatchId(watchId);
        restClient.resource(getMethodUri("exists")).type(MediaType.APPLICATION_JSON).post(existsSpec);

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();
            client.create().creatingParentsIfNeeded().forPath(path);
        }
        finally
        {
            CloseUtil.closeQuietly(client);
        }

        new Timing().sleepABit();

        Status status = restClient.resource(getStatusUri()).get(Status.class);
        boolean foundWatch = false;
        boolean foundWatchId = false;
        boolean foundMessage = false;
        for ( StatusMessage message : status.getMessages() )
        {
            if ( message.getType().equals("watch") )
            {
                foundWatch = true;
                if ( message.getSourceId().equals(watchId) )
                {
                    foundWatchId = true;
                    if ( message.getMessage().equals("NodeCreated") )
                    {
                        foundMessage = true;
                    }
                }
                break;
            }
        }

        Assert.assertTrue(foundWatch);
        Assert.assertTrue(foundWatchId);
        Assert.assertTrue(foundMessage);
    }

    private URI getMethodUri(String method)
    {
        return UriBuilder.fromUri("http://localhost:" + PORT).path(ClientResource.class).path(ClientResource.class, method).build();
    }

    private URI getStatusUri()
    {
        return getMethodUri("getStatus");
    }
}
