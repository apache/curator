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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.entities.Id;
import org.apache.curator.x.rest.entities.PathChildrenCacheSpec;
import org.apache.curator.x.rest.entities.StatusMessage;
import org.codehaus.jackson.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("/curator/v1/recipes/path-cache")
public class PathChildrenCacheResource
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorRestContext context;

    public PathChildrenCacheResource(@Context CuratorRestContext context)
    {
        this.context = context;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response newCache(final PathChildrenCacheSpec spec) throws Exception
    {
        PathChildrenCache cache = new PathChildrenCache(context.getClient(), spec.getPath(), spec.isCacheData(), spec.isDataIsCompressed(), ThreadUtils.newThreadFactory("PathChildrenCacheResource"));
        cache.start(spec.getStartMode());

        Closer<PathChildrenCache> closer = new Closer<PathChildrenCache>()
        {
            @Override
            public void close(PathChildrenCache cache)
            {
                try
                {
                    cache.close();
                }
                catch ( IOException e )
                {
                    log.error("Could not close left-over PathChildrenCache for path: " + spec.getPath(), e);
                }
            }
        };
        final String id = context.getSession().addThing(cache, closer);

        PathChildrenCacheListener listener = new PathChildrenCacheListener()
        {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
            {
                context.getSession().pushMessage(new StatusMessage(Constants.PATH_CACHE, id, event.getType().name().toLowerCase(), event.getData().getPath()));
            }
        };
        cache.getListenable().addListener(listener);

        return Response.ok(new Id(id)).build();
    }

    @DELETE
    @Path("/{cache-id}")
    public Response deleteCache(@PathParam("cache-id") String cacheId)
    {
        PathChildrenCache cache = Constants.deleteThing(context.getSession(), cacheId, PathChildrenCache.class);
        try
        {
            cache.close();
        }
        catch ( IOException e )
        {
            log.error("Could not close PathChildrenCache id: " + cacheId, e);
        }
        return Response.ok().build();
    }

    @GET
    @Path("/{cache-id}")
    public Response getCacheData(@PathParam("cache-id") String cacheId) throws Exception
    {
        PathChildrenCache cache = Constants.getThing(context.getSession(), cacheId, PathChildrenCache.class);

        ArrayNode data = context.getMapper().createArrayNode();
        for ( ChildData c : cache.getCurrentData() )
        {
            data.addPOJO(Constants.toNodeData(c));
        }
        return Response.ok(context.getWriter().writeValueAsString(data)).build();
    }

    @GET
    @Path("/{cache-id}/{path:.*}")
    public Response getCacheDataForPath(@PathParam("cache-id") String cacheId, @PathParam("path") String path) throws Exception
    {
        PathChildrenCache cache = Constants.getThing(context.getSession(), cacheId, PathChildrenCache.class);
        ChildData currentData = cache.getCurrentData("/" + path);
        return Response.ok(Constants.toNodeData(currentData)).build();
    }
}
