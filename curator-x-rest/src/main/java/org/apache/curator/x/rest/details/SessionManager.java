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
package org.apache.curator.x.rest.details;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public class SessionManager implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map<String, Session> sessions = Maps.newConcurrentMap();

    public String newSession()
    {
        String id = newId();
        sessions.put(id, new Session());
        log.debug("Creating session. Id: " + id);
        return id;
    }

    static String newId()
    {
        return UUID.randomUUID().toString();
    }

    @Override
    public void close()
    {
        deleteAllSessions();
    }

    public void deleteAllSessions()
    {
        Collection<Session> localSessions = sessions.values();
        sessions.clear();

        for ( Session session : localSessions )
        {
            session.close();
        }
    }

    public boolean deleteSession(String id)
    {
        Session session = sessions.remove(id);
        if ( session != null )
        {
            log.debug("Deleting session. Id: " + id);
            session.close();
            return true;
        }
        return false;
    }

    public boolean pingSession(String id)
    {
        return getSession(id) != null;
    }

    public Session getSession(String id)
    {
        Session session = sessions.get(id);
        if ( session != null )
        {
            log.debug("Pinging session. Id: " + id);
            session.updateLastUse();
        }
        return session;
    }
}
