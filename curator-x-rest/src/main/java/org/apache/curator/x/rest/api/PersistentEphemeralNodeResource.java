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

import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.x.rest.CuratorRestContext;
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

@Path("/curator/v1/recipes/persistent-ephemeral-node")
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
    public Response start(final PersistentEphemeralNodeSpec spec) throws Exception
    {
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
        String id = context.getSession().addThing(node, closer);
        return Response.ok(context.getWriter().writeValueAsString(Constants.makeIdNode(context, id))).build();
    }

    @DELETE
    @Path("{node-id}")
    public Response close(@PathParam("node-id") String nodeId) throws Exception
    {
        PersistentEphemeralNode node = Constants.deleteThing(context.getSession(), nodeId, PersistentEphemeralNode.class);
        node.close();
        return Response.ok().build();
    }
}
