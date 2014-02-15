package org.apache.curator.x.rest.api;

import com.google.common.base.Joiner;
import org.apache.curator.framework.api.*;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.entities.CreateSpec;
import org.apache.curator.x.rest.entities.DeleteSpec;
import org.apache.curator.x.rest.entities.ExistsSpec;
import org.apache.curator.x.rest.entities.GetChildrenSpec;
import org.apache.curator.x.rest.entities.GetDataSpec;
import org.apache.curator.x.rest.entities.SetDataSpec;
import org.codehaus.jackson.node.ObjectNode;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/curator/v1/client/{session-id}")
public class ClientResource
{
    private final CuratorRestContext context;

    public ClientResource(@Context CuratorRestContext context)
    {
        this.context = context;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/get-children")
    public Response getChildren(@PathParam("session-id") String sessionId, final GetChildrenSpec getChildrenSpec) throws Exception
    {
        Constants.getSession(context, sessionId);

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
    public Response delete(@PathParam("session-id") String sessionId, final DeleteSpec deleteSpec) throws Exception
    {
        Constants.getSession(context, sessionId);

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

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/set-data")
    public Response setData(@PathParam("session-id") String sessionId, final SetDataSpec setDataSpec) throws Exception
    {
        Constants.getSession(context, sessionId);

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
    public Response create(@PathParam("session-id") String sessionId, final CreateSpec createSpec) throws Exception
    {
        Constants.getSession(context, sessionId);

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

        if ( createSpec.isAsync() )
        {
            BackgroundCallback backgroundCallback = new RestBackgroundCallback(context, Constants.CLIENT_CREATE_ASYNC, createSpec.getAsyncId());
            builder = castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        String returnPath = String.valueOf(castBuilder(builder, PathAndBytesable.class).forPath(createSpec.getPath(), createSpec.getData().getBytes()));

        ObjectNode node = context.getMapper().createObjectNode();
        node.put("path", returnPath);
        return Response.ok(context.getWriter().writeValueAsString(node)).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/get-data")
    public Response getData(@PathParam("session-id") String sessionId, final GetDataSpec getDataSpec) throws Exception
    {
        Constants.getSession(context, sessionId);

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
                    return (event.getData() != null) ? new String(event.getData()) : "";
                }
            };
            castBuilder(builder, Backgroundable.class).inBackground(backgroundCallback);
        }

        String result = "";
        Object bytes = castBuilder(builder, Pathable.class).forPath(getDataSpec.getPath());
        if ( bytes != null )
        {
            result = new String((byte[])bytes);
        }

        ObjectNode node = context.getMapper().createObjectNode();
        node.put("data", result);
        return Response.ok(context.getWriter().writeValueAsString(node)).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/exists")
    public Response exists(@PathParam("session-id") String sessionId, final ExistsSpec existsSpec) throws Exception
    {
        Constants.getSession(context, sessionId);

        Object builder = context.getClient().checkExists();
        if ( existsSpec.isWatched() )
        {
            builder = castBuilder(builder, Watchable.class).usingWatcher(new RestWatcher(context, existsSpec.getWatchId()));
        }

        if ( existsSpec.isAsync() )
        {
            BackgroundCallback backgroundCallback = new RestBackgroundCallback(context, Constants.CLIENT_EXISTS_ASYNC, existsSpec.getAsyncId());
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

}
