package org.apache.curator.x.rest.api;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.details.Closer;
import org.apache.curator.x.rest.details.Session;
import org.apache.curator.x.rest.entities.PathChildrenCacheSpec;
import org.apache.curator.x.rest.entities.StatusMessage;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
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

@Path("/curator/v1/recipes/path-cache/{session-id}")
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
    public Response newCache(@PathParam("session-id") String sessionId, final PathChildrenCacheSpec spec) throws Exception
    {
        Session session = Constants.getSession(context, sessionId);

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
        final String id = session.addThing(cache, closer);

        PathChildrenCacheListener listener = new PathChildrenCacheListener()
        {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
            {
                context.pushMessage(new StatusMessage(Constants.PATH_CACHE, id, event.getType().name(), ""));
            }
        };
        cache.getListenable().addListener(listener);

        ObjectNode node = Constants.makeIdNode(context, id);
        return Response.ok(context.getWriter().writeValueAsString(node)).build();
    }

    @DELETE
    @Path("/{cache-id}")
    public Response deleteCache(@PathParam("session-id") String sessionId, @PathParam("cache-id") String cacheId)
    {
        Session session = Constants.getSession(context, sessionId);
        PathChildrenCache cache = Constants.deleteThing(session, cacheId, PathChildrenCache.class);
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
    public Response getCacheData(@PathParam("session-id") String sessionId, @PathParam("cache-id") String cacheId) throws Exception
    {
        Session session = Constants.getSession(context, sessionId);
        PathChildrenCache cache = Constants.getThing(session, cacheId, PathChildrenCache.class);

        ArrayNode data = context.getMapper().createArrayNode();
        for ( ChildData c : cache.getCurrentData() )
        {
            data.addPOJO(Constants.toNodeData(c));
        }
        return Response.ok(context.getWriter().writeValueAsString(data)).build();
    }

    @GET
    @Path("/{cache-id}/{path:.*}")
    public Response getCacheDataForPath(@PathParam("session-id") String sessionId, @PathParam("cache-id") String cacheId, @PathParam("path") String path) throws Exception
    {
        Session session = Constants.getSession(context, sessionId);
        PathChildrenCache cache = Constants.getThing(session, cacheId, PathChildrenCache.class);
        ChildData currentData = cache.getCurrentData(path);
        if ( currentData == null )
        {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        return Response.ok(Constants.toNodeData(currentData)).build();
    }
}
