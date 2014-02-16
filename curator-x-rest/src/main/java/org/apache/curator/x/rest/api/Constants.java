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

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.x.rest.CuratorRestContext;
import org.apache.curator.x.rest.entities.NodeData;
import org.codehaus.jackson.node.ObjectNode;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.util.UUID;

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
    static final String CLOSING = "closing";

    static ObjectNode makeIdNode(CuratorRestContext context, String id)
    {
        ObjectNode node = context.getMapper().createObjectNode();
        node.put("id", id);
        return node;
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

    public static String newId()
    {
        return UUID.randomUUID().toString();
    }
}
