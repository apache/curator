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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.entities.LeaderSpec;
import org.apache.curator.x.rest.entities.ParticipantSpec;
import org.apache.curator.x.rest.entities.StatusMessage;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

@Path("/curator/v1/recipes/leader")
public class LeaderResource
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorRestContext context;

    public LeaderResource(@Context CuratorRestContext context)
    {
        this.context = context;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response startLeaderSelection(final LeaderSpec leaderSpec) throws Exception
    {
        LeaderLatch leaderLatch = new LeaderLatch(context.getClient(), leaderSpec.getPath(), leaderSpec.getParticipantId());
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
        final String id = context.getSession().addThing(leaderLatch, closer);

        LeaderLatchListener listener = new LeaderLatchListener()
        {
            @Override
            public void isLeader()
            {
                context.getSession().pushMessage(new StatusMessage(Constants.LEADER, id, "true", ""));
            }

            @Override
            public void notLeader()
            {
                context.getSession().pushMessage(new StatusMessage(Constants.LEADER, id, "false", ""));
            }
        };
        leaderLatch.addListener(listener);

        ObjectNode node = Constants.makeIdNode(context, id);
        return Response.ok(context.getWriter().writeValueAsString(node)).build();
    }

    @DELETE
    @Path("{leader-id}")
    public Response closeLeader(@PathParam("leader-id") String leaderId) throws Exception
    {
        LeaderLatch leaderLatch = Constants.deleteThing(context.getSession(), leaderId, LeaderLatch.class);
        leaderLatch.close();
        return Response.ok().build();
    }

    @GET
    @Path("{leader-id}")
    public Response getParticipants(@PathParam("leader-id") String leaderId) throws Exception
    {
        LeaderLatch leaderLatch = Constants.getThing(context.getSession(), leaderId, LeaderLatch.class);
        Collection<Participant> participants = leaderLatch.getParticipants();

        List<ParticipantSpec> transformed = Lists.transform
        (
            Lists.newArrayList(participants),
            new Function<Participant, ParticipantSpec>()
            {
                @Nullable
                @Override
                public ParticipantSpec apply(Participant participant)
                {
                    return new ParticipantSpec(participant.getId(), participant.isLeader());
                }
            }
        );

        GenericEntity<List<ParticipantSpec>> entity = new GenericEntity<List<ParticipantSpec>>(transformed){};
        return Response.ok(entity).build();
    }
}
