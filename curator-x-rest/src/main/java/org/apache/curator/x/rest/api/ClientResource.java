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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.*;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.entities.CreateSpec;
import org.apache.curator.x.rest.entities.DeleteSpec;
import org.apache.curator.x.rest.entities.ExistsSpec;
import org.apache.curator.x.rest.entities.GetChildrenSpec;
import org.apache.curator.x.rest.entities.GetDataSpec;
import org.apache.curator.x.rest.entities.PathAndId;
import org.apache.curator.x.rest.entities.SetDataSpec;
import org.apache.curator.x.rest.entities.Status;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;

@Path("/curator/v1/client")
public class ClientResource
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorRestContext context;

    public ClientResource(@Context CuratorRestContext context)
    {
        this.context = context;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/status")
    public Response getStatus() throws IOException
    {
        return getStatusWithTouch(ImmutableList.<String>of());
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/status")
    public Response getStatusWithTouch(List<String> ids) throws IOException
    {
        Status status = new Status(context.getConnectionState().name().toLowerCase(), context.getSession().drainMessages());

        for ( String id : ids )
        {
            context.getSession().updateThingLastUse(id);
        }

        return Response.ok(status).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/get-children")
    public Response getChildren(final GetChildrenSpec getChildrenSpec) throws Exception
    {
        Object builder = context.getClient().getChildren();
        if ( getChildrenSpec.isWatched() )
        {
            builder = castBuilder(builder, Watchable.class).usingWatcher(new RestWatcher(context, getChildrenSpec.getWatchId()));
        }

        if ( getChildrenSpec.isAsync() )
        {
            BackgroundCallback backgroundCallback = new RestBackgroundCallback(context, Constants.CLIENT_GET_CHILDREN_ASYNC, getChildrenSpec.getAsyncId())
            {
                @Override
                protected String getMessage(CuratorEvent event)
                {
                    List<String> children = event.getChildren();
                    if ( children != null )
                    {
                        return Joiner.on(getChildrenSpec.getAsyncListSeparator()).join(children);
                    }
                    return "";
                }
            };
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        Object children = castBuilder(builder, Pathable.class).forPath(getChildrenSpec.getPath());
        if ( children != null )
        {
            return Response.ok(children).build();
        }
        return Response.ok().build();
    }

    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/delete")
    public Response delete(final DeleteSpec deleteSpec) throws Exception
    {
        Object builder = context.getClient().delete();
        if ( deleteSpec.isGuaranteed() )
        {
            builder = castBuilder(builder, DeleteBuilder.class).guaranteed();
        }
        builder = castBuilder(builder, Versionable.class).withVersion(deleteSpec.getVersion());

        if ( deleteSpec.isAsync() )
        {
            BackgroundCallback backgroundCallback = new RestBackgroundCallback(context, Constants.CLIENT_DELETE_ASYNC, deleteSpec.getAsyncId());
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        castBuilder(builder, Pathable.class).forPath(deleteSpec.getPath());
        return Response.ok().build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/set-data")
    public Response setData(final SetDataSpec setDataSpec) throws Exception
    {
        Object builder = context.getClient().setData();
        if ( setDataSpec.isCompressed() )
        {
            builder = castBuilder(builder, Compressible.class).compressed();
        }
        if ( setDataSpec.isWatched() )
        {
            builder = castBuilder(builder, Watchable.class).usingWatcher(new RestWatcher(context, setDataSpec.getWatchId()));
        }
        builder = castBuilder(builder, Versionable.class).withVersion(setDataSpec.getVersion());

        if ( setDataSpec.isAsync() )
        {
            BackgroundCallback backgroundCallback = new RestBackgroundCallback(context, Constants.CLIENT_SET_DATA_ASYNC, setDataSpec.getAsyncId());
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        castBuilder(builder, PathAndBytesable.class).forPath(setDataSpec.getPath(), setDataSpec.getData().getBytes());
        return Response.ok().build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/create")
    public Response create(final CreateSpec createSpec) throws Exception
    {
        Object builder = context.getClient().create();
        if ( createSpec.isCreatingParentsIfNeeded() )
        {
            builder = castBuilder(builder, CreateBuilder.class).creatingParentsIfNeeded();
        }
        if ( createSpec.isCompressed() )
        {
            builder = castBuilder(builder, Compressible.class).compressed();
        }
        if ( createSpec.isWithProtection() )
        {
            builder = castBuilder(builder, CreateBuilder.class).withProtection();
        }
        builder = castBuilder(builder, CreateModable.class).withMode(createSpec.getMode());

        final String id = Constants.newId();
        if ( createSpec.isAsync() )
        {
            BackgroundCallback backgroundCallback = new RestBackgroundCallback(context, Constants.CLIENT_CREATE_ASYNC, createSpec.getAsyncId())
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    if ( event.getResultCode() == 0 )
                    {
                        checkEphemeralCreate(createSpec, id, event.getName());
                    }
                    super.processResult(client, event);
                }

                @Override
                protected String getMessage(CuratorEvent event)
                {
                    PathAndId pathAndId = new PathAndId(String.valueOf(event.getName()), id);
                    try
                    {
                        return context.getWriter().writeValueAsString(pathAndId);
                    }
                    catch ( IOException e )
                    {
                        log.error("Could not serialize PathAndId", e);
                    }
                    return "{}";
                }

                @Override
                protected String getDetails(CuratorEvent event)
                {
                    if ( event.getResultCode() != 0 )
                    {
                        return super.getDetails(event);
                    }
                    return id;
                }
            };
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        String returnPath = String.valueOf(castBuilder(builder, PathAndBytesable.class).forPath(createSpec.getPath(), createSpec.getData().getBytes()));
        checkEphemeralCreate(createSpec, id, returnPath);
        return Response.ok(new PathAndId(returnPath, id)).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/get-data")
    public Response getData(final GetDataSpec getDataSpec) throws Exception
    {
        Object builder = context.getClient().getData();
        if ( getDataSpec.isWatched() )
        {
            builder = castBuilder(builder, Watchable.class).usingWatcher(new RestWatcher(context, getDataSpec.getWatchId()));
        }
        if ( getDataSpec.isDecompressed() )
        {
            builder = castBuilder(builder, Decompressible.class).decompressed();
        }

        if ( getDataSpec.isAsync() )
        {
            BackgroundCallback backgroundCallback = new RestBackgroundCallback(context, Constants.CLIENT_GET_DATA_ASYNC, getDataSpec.getAsyncId())
            {
                @Override
                protected String getMessage(CuratorEvent event)
                {
                    if ( event.getResultCode() == 0 )
                    {
                        return (event.getData() != null) ? new String(event.getData()) : "";
                    }
                    return "";
                }
            };
            castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        Stat stat = new Stat();
        builder = castBuilder(builder, Statable.class).storingStatIn(stat);

        String result = "";
        Object bytes = castBuilder(builder, Pathable.class).forPath(getDataSpec.getPath());
        if ( bytes != null )
        {
            result = new String((byte[])bytes);
        }

        ObjectNode node = context.getMapper().createObjectNode();
        node.put("data", result);
        node.putPOJO("stat", stat);
        return Response.ok(context.getWriter().writeValueAsString(node)).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/exists")
    public Response exists(final ExistsSpec existsSpec) throws Exception
    {
        Object builder = context.getClient().checkExists();
        if ( existsSpec.isWatched() )
        {
            builder = castBuilder(builder, Watchable.class).usingWatcher(new RestWatcher(context, existsSpec.getWatchId()));
        }

        if ( existsSpec.isAsync() )
        {
            BackgroundCallback backgroundCallback = new RestBackgroundCallback(context, Constants.CLIENT_EXISTS_ASYNC, existsSpec.getAsyncId())
            {
                @Override
                protected String getMessage(CuratorEvent event)
                {
                    Stat stat = event.getStat();
                    if ( stat != null )
                    {
                        try
                        {
                            return context.getWriter().writeValueAsString(stat);
                        }
                        catch ( IOException e )
                        {
                            log.error("Could not serialize stat object", e);
                        }
                    }
                    return "{}";
                }
            };
            castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        Object stat = castBuilder(builder, Pathable.class).forPath(existsSpec.getPath());
        if ( stat != null )
        {
            return Response.ok(stat).build();
        }

        return Response.ok("{}").build();
    }

    private static <T> T castBuilder(Object createBuilder, Class<T> clazz)
    {
        if ( clazz.isAssignableFrom(createBuilder.getClass()) )
        {
            return clazz.cast(createBuilder);
        }
        throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    private void checkEphemeralCreate(CreateSpec createSpec, String id, String ephemeralPath)
    {
        if ( (createSpec.getMode() == CreateMode.EPHEMERAL) || (createSpec.getMode() == CreateMode.EPHEMERAL_SEQUENTIAL) )
        {
            Closer<String> closer = new Closer<String>()
            {
                @Override
                public void close(String path)
                {
                    log.warn("Ephemeral node has expired and is being deleted: " + path);
                    try
                    {
                        context.getClient().delete().guaranteed().inBackground().forPath(path);
                    }
                    catch ( Exception e )
                    {
                        log.error("Could not delete expired ephemeral node: " + path);
                    }
                }
            };
            context.getSession().addThing(id, ephemeralPath, closer);
        }
    }
}
