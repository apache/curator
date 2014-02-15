package org.apache.curator.x.rest.api;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.details.Closer;
import org.apache.curator.x.rest.details.Session;
import org.apache.curator.x.rest.entities.LeaderSpec;
import org.apache.curator.x.rest.entities.StatusMessage;
import org.codehaus.jackson.node.ObjectNode;
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
import java.io.IOException;

@Path("/curator/v1/recipes/leader/{session-id}")
public class LeaderResource
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorRestContext context;

    public LeaderResource(@Context CuratorRestContext context)
    {
        this.context = context;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response startLeaderSelection(@PathParam("session-id") String sessionId, final LeaderSpec leaderSpec) throws Exception
    {
        Session session = Constants.getSession(context, sessionId);

        LeaderLatch leaderLatch = new LeaderLatch(context.getClient(), leaderSpec.getPath(), leaderSpec.getId());
        leaderLatch.start();

        Closer<LeaderLatch> closer = new Closer<LeaderLatch>()
        {
            @Override
            public void close(LeaderLatch latch)
            {
                try
                {
                    latch.close();
                }
                catch ( IOException e )
                {
                    log.error("Could not close left-over leader latch for path: " + leaderSpec.getPath(), e);
                }
            }
        };
        final String id = session.addThing(leaderLatch, closer);

        LeaderLatchListener listener = new LeaderLatchListener()
        {
            @Override
            public void isLeader()
            {
                context.pushMessage(new StatusMessage(Constants.LEADER, id, "true", ""));
            }

            @Override
            public void notLeader()
            {
                context.pushMessage(new StatusMessage(Constants.LEADER, id, "false", ""));
            }
        };
        leaderLatch.addListener(listener);

        ObjectNode node = Constants.makeIdNode(context, id);
        return Response.ok(context.getWriter().writeValueAsString(node)).build();
    }

    @DELETE
    @Path("{leader-id}")
    public Response closeLeader(@PathParam("session-id") String sessionId, @PathParam("leader-id") String leaderId) throws Exception
    {
        Session session = Constants.getSession(context, sessionId);
        LeaderLatch leaderLatch = Constants.deleteThing(session, leaderId, LeaderLatch.class);
        leaderLatch.close();
        return Response.ok().build();
    }
}
