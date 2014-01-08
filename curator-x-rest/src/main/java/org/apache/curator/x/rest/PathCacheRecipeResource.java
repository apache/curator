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

package org.apache.curator.x.rest;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.x.rest.entity.PathChildrenCacheEntity;
import org.apache.curator.x.rest.entity.PathChildrenCacheEventEntity;
import org.apache.curator.x.rest.entity.StatEntity;
import org.apache.curator.x.rest.system.Connection;
import org.apache.curator.x.rest.system.ConnectionsManager;
import org.apache.curator.x.rest.system.PathChildrenCacheThing;
import org.apache.curator.x.rest.system.ThingKey;
import org.apache.curator.x.rest.system.ThingType;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ContextResolver;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

@Path("zookeeper/recipes/path-cache/{connectionId}")
public class PathCacheRecipeResource
{
    private final ConnectionsManager connectionsManager;

    public PathCacheRecipeResource(@Context ContextResolver<ConnectionsManager> contextResolver)
    {
        connectionsManager = contextResolver.getContext(ConnectionsManager.class);
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response allocate(@PathParam("connectionId") String connectionId, PathChildrenCacheEntity spec) throws Exception
    {
        Connection connection = connectionsManager.get(connectionId);
        if ( connection == null )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        PathChildrenCache cache = new PathChildrenCache(connection.getClient(), spec.getPath(), spec.isCacheData(), spec.isDataIsCompressed());
        PathChildrenCacheThing cacheThing = new PathChildrenCacheThing(cache);
        cache.getListenable().addListener(new LocalListener(cacheThing));

        ThingKey<PathChildrenCacheThing> key = new ThingKey<PathChildrenCacheThing>(ThingType.PATH_CACHE);
        connection.putThing(key, cacheThing);

        PathChildrenCache.StartMode startMode = spec.isBuildInitial() ? PathChildrenCache.StartMode.POST_INITIALIZED_EVENT : PathChildrenCache.StartMode.NORMAL;
        cache.start(startMode);

        return Response.ok(key.getId()).build();
    }

    @DELETE
    @Path("{id}")
    public Response delete(@PathParam("connectionId") String connectionId, @PathParam("id") String cacheId) throws IOException
    {
        Connection connection = connectionsManager.get(connectionId);
        if ( connection == null )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        PathChildrenCacheThing cacheThing = connection.removeThing(new ThingKey<PathChildrenCacheThing>(cacheId, ThingType.PATH_CACHE));
        if ( cacheThing == null )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        cacheThing.getCache().close();

        return Response.ok().build();
    }

    @GET
    @Path("{id}/block-on-events")
    public void getEvents(@Suspended final AsyncResponse asyncResponse, @PathParam("connectionId") String connectionId, @PathParam("id") String cacheId)
    {
        Connection connection = connectionsManager.get(connectionId);
        if ( connection == null )
        {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
            return;
        }

        final PathChildrenCacheThing cacheThing = connection.removeThing(new ThingKey<PathChildrenCacheThing>(cacheId, ThingType.PATH_CACHE));
        if ( cacheThing == null )
        {
            asyncResponse.resume(Response.status(Response.Status.NOT_FOUND).build());
            return;
        }

        Future<?> future = connectionsManager.getExecutorService().submit
        (
            new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        List<PathChildrenCacheEvent> events = cacheThing.blockForPendingEvents();
                        List<PathChildrenCacheEventEntity> transformed = Lists.transform(events, toEntity);
                        GenericEntity<List<PathChildrenCacheEventEntity>> entity = new GenericEntity<List<PathChildrenCacheEventEntity>>(transformed){};
                        asyncResponse.resume(Response.ok(entity).build());
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                        asyncResponse.resume(Response.status(Response.Status.SERVICE_UNAVAILABLE).build());
                    }
                }
            }
        );
        connection.putThing(new ThingKey<Future>(ThingType.FUTURE), future);
    }

    private static final Function<PathChildrenCacheEvent, PathChildrenCacheEventEntity> toEntity = new Function<PathChildrenCacheEvent, PathChildrenCacheEventEntity>()
    {
        @Override
        public PathChildrenCacheEventEntity apply(PathChildrenCacheEvent event)
        {
            String path = (event.getData() != null) ? event.getData().getPath() : null;
            String data = ((event.getData() != null) && (event.getData().getData() != null)) ? new String((event.getData().getData())) : null;
            StatEntity stat = ((event.getData() != null) && (event.getData().getStat() != null))
                ? new StatEntity
                (
                    event.getData().getStat().getCzxid(),
                    event.getData().getStat().getMzxid(),
                    event.getData().getStat().getCtime(),
                    event.getData().getStat().getMtime(),
                    event.getData().getStat().getVersion(),
                    event.getData().getStat().getCversion(),
                    event.getData().getStat().getAversion(),
                    event.getData().getStat().getEphemeralOwner(),
                    event.getData().getStat().getDataLength(),
                    event.getData().getStat().getNumChildren(),
                    event.getData().getStat().getPzxid()
                )
                : null;
            return new PathChildrenCacheEventEntity
            (
                event.getType().name(),
                path,
                data,
                stat
            );
        }
    };

    private static class LocalListener implements PathChildrenCacheListener
    {
        private final PathChildrenCacheThing cacheThing;

        public LocalListener(PathChildrenCacheThing cacheThing)
        {
            this.cacheThing = cacheThing;
        }

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
        {
            cacheThing.addEvent(event);
        }
    }
}
