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

package org.apache.curator.x.rest.support;

import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.x.rest.api.LockResource;
import org.apache.curator.x.rest.entities.Id;
import org.apache.curator.x.rest.entities.LockSpec;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.concurrent.TimeUnit;

public class InterProcessLockBridge implements InterProcessLock
{
    private final Client restClient;
    private final SessionManager sessionManager;
    private final UriMaker uriMaker;

    private volatile String id = null;

    private static final String PATH = "/lock";

    public InterProcessLockBridge(Client restClient, SessionManager sessionManager, UriMaker uriMaker)
    {
        this.restClient = restClient;
        this.sessionManager = sessionManager;
        this.uriMaker = uriMaker;
    }

    @Override
    public void acquire() throws Exception
    {
        acquire(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean acquire(long time, TimeUnit unit) throws Exception
    {
        Preconditions.checkArgument(id == null, "Lock already acquired");

        URI uri = uriMaker.getMethodUri(LockResource.class, null);
        LockSpec lockSpec = new LockSpec();
        lockSpec.setPath(PATH);
        lockSpec.setMaxWaitMs((int)unit.toMillis(time));
        try
        {
            id = restClient.resource(uri).type(MediaType.APPLICATION_JSON_TYPE).post(Id.class, lockSpec).getId();
        }
        catch ( UniformInterfaceException e )
        {
            if ( e.getResponse().getStatus() == Response.Status.SERVICE_UNAVAILABLE.getStatusCode() )
            {
                return false;
            }
            throw e;
        }
        sessionManager.addEntry(uriMaker.getLocalhost(), id, null);
        return true;
    }

    @Override
    public void release() throws Exception
    {
        Preconditions.checkArgument(id != null, "Lock not acquired");

        URI uri = uriMaker.getMethodUri(LockResource.class, null);
        restClient.resource(uri).path(id).delete();

        sessionManager.removeEntry(uriMaker.getLocalhost(), id);
        id = null;
    }

    @Override
    public boolean isAcquiredInThisProcess()
    {
        return (id != null);
    }
}
