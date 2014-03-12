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

import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.rest.entities.CreateSpec;
import org.apache.curator.x.rest.entities.DeleteSpec;
import org.apache.curator.x.rest.entities.PathAndId;
import org.apache.curator.x.rest.entities.SetDataSpec;
import org.apache.curator.x.rest.support.BaseClassForTests;
import org.apache.curator.x.rest.support.PathChildrenCacheBridge;
import org.testng.Assert;
import org.testng.annotations.Test;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestPathCache extends BaseClassForTests
{
    @Test
    public void testBasics() throws Exception
    {
        CreateSpec createSpec = new CreateSpec();

        createSpec.setPath("/test");
        restClient.resource(uriMaker.getMethodUri("create")).type(MediaType.APPLICATION_JSON).post(PathAndId.class, createSpec);

        final BlockingQueue<String> events = new LinkedBlockingQueue<String>();
        PathChildrenCacheBridge cache = new PathChildrenCacheBridge(restClient, sessionManager, uriMaker, "/test", true, false);
        try
        {
            cache.getListenable().addListener
            (
                new PathChildrenCacheBridge.Listener()
                {
                    @Override
                    public void childEvent(String event, String path) throws Exception
                    {
                        if ( path.equals("/test/one") )
                        {
                            events.offer(event);
                        }
                    }
                }
            );
            cache.start();

            createSpec.setPath("/test/one");
            createSpec.setData("hey there");
            restClient.resource(uriMaker.getMethodUri("create")).type(MediaType.APPLICATION_JSON).post(PathAndId.class, createSpec);
            Assert.assertEquals(events.poll(10, TimeUnit.SECONDS), "child_added");

            SetDataSpec setDataSpec = new SetDataSpec();
            setDataSpec.setPath("/test/one");
            setDataSpec.setData("sup!");
            restClient.resource(uriMaker.getMethodUri("setData")).type(MediaType.APPLICATION_JSON).post(setDataSpec);
            Assert.assertEquals(events.poll(10, TimeUnit.SECONDS), "child_updated");
            Assert.assertEquals(cache.getCurrentData("/test/one").getNodeData().getData(), "sup!");

            DeleteSpec deleteSpec = new DeleteSpec();
            deleteSpec.setPath("/test/one");
            restClient.resource(uriMaker.getMethodUri("delete")).type(MediaType.APPLICATION_JSON).post(deleteSpec);
            Assert.assertEquals(events.poll(10, TimeUnit.SECONDS), "child_removed");
        }
        finally
        {
            CloseableUtils.closeQuietly(cache);
        }
    }
}
