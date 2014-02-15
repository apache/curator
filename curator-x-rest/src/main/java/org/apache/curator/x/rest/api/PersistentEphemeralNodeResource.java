package org.apache.curator.x.rest.api;

import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.details.Closer;
import org.apache.curator.x.rest.details.Session;
import org.apache.curator.x.rest.entities.PersistentEphemeralNodeSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/curator/v1/recipes/persistent-ephemeral-node/{session-id}")
public class PersistentEphemeralNodeResource
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorRestContext context;

    public PersistentEphemeralNodeResource(@Context CuratorRestContext context)
    {
        this.context = context;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response start(@PathParam("session-id") String sessionId, final PersistentEphemeralNodeSpec spec) throws Exception
    {
        Session session = Constants.getSession(context, sessionId);
        PersistentEphemeralNode node = new PersistentEphemeralNode(context.getClient(), spec.getMode(), spec.getPath(), spec.getData().getBytes());
        node.start();

        Closer<PersistentEphemeralNode> closer = new Closer<PersistentEphemeralNode>()
        {
            @Override
            public void close(PersistentEphemeralNode node)
            {
                try
                {
                    node.close();
                }
                catch ( Exception e )
                {
                    log.error("Could not release left-over persistent ephemeral node for path: " + spec.getPath(), e);
                }
            }
        };
        String id = session.addThing(node, closer);
        return Response.ok(context.getWriter().writeValueAsString(Constants.makeIdNode(context, id))).build();
    }

    @DELETE
    @Path("{node-id}")
    public Response close(@PathParam("session-id") String sessionId, @PathParam("node-id") String nodeId) throws Exception
    {
        Session session = Constants.getSession(context, sessionId);
        PersistentEphemeralNode node = Constants.deleteThing(session, nodeId, PersistentEphemeralNode.class);
        node.close();
        return Response.ok().build();
    }
}
