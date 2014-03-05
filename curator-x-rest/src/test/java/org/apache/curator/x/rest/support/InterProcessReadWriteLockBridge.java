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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.x.rest.api.ReadWriteLockResource;
import org.apache.curator.x.rest.entities.Id;
import org.apache.curator.x.rest.entities.LockSpec;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.concurrent.TimeUnit;

public class InterProcessReadWriteLockBridge
{
    private final Client restClient;
    private final SessionManager sessionManager;
    private final UriMaker uriMaker;
    private final String path;

    public InterProcessReadWriteLockBridge(Client restClient, SessionManager sessionManager, UriMaker uriMaker, String path)
    {

        this.restClient = restClient;
        this.sessionManager = sessionManager;
        this.uriMaker = uriMaker;
        this.path = path;
    }

    public InterProcessLock writeLock()
    {
        return new InternalLock(true);
    }

    public InterProcessLock readLock()
    {
        return new InternalLock(false);
    }

    private class InternalLock implements InterProcessLock
    {
        private final boolean writeLock;
        private final ThreadLocal<String> id = new ThreadLocal<String>();

        public InternalLock(boolean writeLock)
        {
            this.writeLock = writeLock;
        }

        @Override
        public void acquire() throws Exception
        {
            if ( !acquire(Integer.MAX_VALUE, TimeUnit.MILLISECONDS) )
            {
                throw new Exception("Could not acquire");
            }
        }

        @Override
        public boolean acquire(long time, TimeUnit unit) throws Exception
        {
            if ( id.get() != null )
            {
                throw new Exception("Already acquired in this thread");
            }

            URI uri = uriMaker.getMethodUri(ReadWriteLockResource.class, writeLock ? "acquireWriteLock" : "acquireReadLock");
            LockSpec lockSpec = new LockSpec();
            lockSpec.setPath(path);
            lockSpec.setMaxWaitMs((int)unit.toMillis(time));

            String id;
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

            this.id.set(id);
            sessionManager.addEntry(uriMaker.getLocalhost(), id, null);
            return true;
        }

        @Override
        public void release() throws Exception
        {
            String localId = id.get();
            if ( localId == null )
            {
                throw new Exception("Not acquired in this thread");
            }

            URI uri = uriMaker.getMethodUri(ReadWriteLockResource.class, null);
            restClient.resource(uri).path(localId).delete();

            sessionManager.removeEntry(uriMaker.getLocalhost(), localId);
            id.set(null);
        }

        @Override
        public boolean isAcquiredInThisProcess()
        {
            return id.get() != null;
        }
    }
}
