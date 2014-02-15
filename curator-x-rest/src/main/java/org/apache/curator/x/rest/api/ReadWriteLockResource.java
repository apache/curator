package org.apache.curator.x.rest.api;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.details.Closer;
import org.apache.curator.x.rest.details.Session;
import org.apache.curator.x.rest.entities.LockSpec;
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
import java.util.concurrent.TimeUnit;

@Path("/curator/v1/recipes/read-write-lock/{session-id}")
public class ReadWriteLockResource
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorRestContext context;

    public ReadWriteLockResource(@Context CuratorRestContext context)
    {
        this.context = context;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/read")
    public Response acquireReadLock(@PathParam("session-id") String sessionId, final LockSpec lockSpec) throws Exception
    {
        return internalLock(sessionId, lockSpec, false);
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/write")
    public Response acquireWriteLock(@PathParam("session-id") String sessionId, final LockSpec lockSpec) throws Exception
    {
        return internalLock(sessionId, lockSpec, true);
    }

    private Response internalLock(String sessionId, final LockSpec lockSpec, boolean writeLock) throws Exception
    {
        Session session = Constants.getSession(context, sessionId);
        InterProcessReadWriteLock lock = new InterProcessReadWriteLock(context.getClient(), lockSpec.getPath());
        InterProcessMutex actualLock = writeLock ? lock.writeLock() : lock.readLock();
        if ( !actualLock.acquire(lockSpec.getMaxWaitMs(), TimeUnit.MILLISECONDS) )
        {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        }

        Closer<InterProcessMutex> closer = new Closer<InterProcessMutex>()
        {
            @Override
            public void close(InterProcessMutex lock)
            {
                if ( lock.isAcquiredInThisProcess() )
                {
                    try
                    {
                        lock.release();
                    }
                    catch ( Exception e )
                    {
                        log.error("Could not release left-over read/write lock for path: " + lockSpec.getPath(), e);
                    }
                }
            }
        };
        String id = session.addThing(actualLock, closer);
        ObjectNode node = Constants.makeIdNode(context, id);
        return Response.ok(context.getWriter().writeValueAsString(node)).build();
    }

    @DELETE
    @Path("{lock-id}")
    public Response releaseLock(@PathParam("session-id") String sessionId, @PathParam("lock-id") String lockId) throws Exception
    {
        Session session = Constants.getSession(context, sessionId);
        InterProcessMutex lock = Constants.deleteThing(session, lockId, InterProcessMutex.class);
        lock.release();
        return Response.ok().build();
    }
}
