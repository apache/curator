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
