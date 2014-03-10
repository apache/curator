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

import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.test.Timing;
import org.apache.curator.x.rest.entities.CreateSpec;
import org.apache.curator.x.rest.entities.DeleteSpec;
import org.apache.curator.x.rest.entities.PathAndId;
import org.apache.curator.x.rest.entities.SetDataSpec;
import org.apache.curator.x.rest.support.BaseClassForTests;
import org.apache.curator.x.rest.support.NodeCacheBridge;
import org.testng.Assert;
import org.testng.annotations.Test;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.Semaphore;

public class TestNodeCache extends BaseClassForTests
{
    @Test
    public void     testBasics() throws Exception
    {
        Timing timing = new Timing();

        CreateSpec createSpec = new CreateSpec();

        createSpec.setPath("/test");
        restClient.resource(uriMaker.getMethodUri("create")).type(MediaType.APPLICATION_JSON).post(PathAndId.class, createSpec);

        NodeCacheBridge cache = new NodeCacheBridge(restClient, sessionManager, uriMaker, "/test/node");
        cache.start(true);

        final Semaphore semaphore = new Semaphore(0);
        cache.getListenable().addListener
            (
                new NodeCacheListener()
                {
                    @Override
                    public void nodeChanged() throws Exception
                    {
                        semaphore.release();
                    }
                }
            );

        Assert.assertNull(cache.getCurrentData());

        createSpec.setPath("/test/node");
        createSpec.setData("a");
        restClient.resource(uriMaker.getMethodUri("create")).type(MediaType.APPLICATION_JSON).post(PathAndId.class, createSpec);
        Assert.assertTrue(timing.acquireSemaphore(semaphore));
        Assert.assertEquals(cache.getCurrentData().getData(), "a".getBytes());

        SetDataSpec setDataSpec = new SetDataSpec();
        setDataSpec.setPath("/test/node");
        setDataSpec.setData("b");
        restClient.resource(uriMaker.getMethodUri("setData")).type(MediaType.APPLICATION_JSON).post(setDataSpec);
        Assert.assertTrue(timing.acquireSemaphore(semaphore));
        Assert.assertEquals(cache.getCurrentData().getData(), "b".getBytes());

        DeleteSpec deleteSpec = new DeleteSpec();
        deleteSpec.setPath("/test/node");
        restClient.resource(uriMaker.getMethodUri("delete")).type(MediaType.APPLICATION_JSON).post(deleteSpec);
        Assert.assertTrue(timing.acquireSemaphore(semaphore));
        Assert.assertNull(cache.getCurrentData());
    }
}
