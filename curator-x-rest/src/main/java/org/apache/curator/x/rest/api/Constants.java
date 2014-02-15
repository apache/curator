package org.apache.curator.x.rest.api;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.details.Session;
import org.apache.curator.x.rest.entities.NodeData;
import org.codehaus.jackson.node.ObjectNode;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

class Constants
{
    static final String CLIENT_CREATE_ASYNC = "client-create-async";
    static final String CLIENT_GET_DATA_ASYNC = "client-get-data-async";
    static final String CLIENT_GET_CHILDREN_ASYNC = "client-get-children-async";
    static final String CLIENT_SET_DATA_ASYNC = "client-set-data-async";
    static final String CLIENT_EXISTS_ASYNC = "client-exists-async";
    static final String CLIENT_DELETE_ASYNC = "client-delete-async";
    static final String WATCH = "watch";
    static final String PATH_CACHE = "path-cache";
    static final String NODE_CACHE = "node-cache";
    static final String LEADER = "leader";

    static ObjectNode makeIdNode(CuratorRestContext context, String id)
    {
        ObjectNode node = context.getMapper().createObjectNode();
        node.put("id", id);
        return node;
    }

    static Session getSession(CuratorRestContext context, String sessionId)
    {
        Session session = context.getSessionManager().getSession(sessionId);
        if ( session == null )
        {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        return session;
    }

    static <T> T getThing(Session session, String id, Class<T> clazz)
    {
        T thing = session.getThing(id, clazz);
        if ( thing == null )
        {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        return thing;
    }

    static <T> T deleteThing(Session session, String id, Class<T> clazz)
    {
        T thing = session.deleteThing(id, clazz);
        if ( thing == null )
        {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        return thing;
    }

    private Constants()
    {
    }

    static NodeData toNodeData(ChildData c)
    {
        String payload = (c.getData() != null) ? new String(c.getData()) : "";
        return new NodeData(c.getPath(), c.getStat(), payload);
    }
}
