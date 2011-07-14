/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator;

import com.netflix.curator.drivers.LoggingDriver;
import com.netflix.curator.drivers.TracerDriver;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class ConnectionState implements Watcher, Closeable
{
    private volatile long connectionStartMs = 0;
    private volatile long sessionId = 0;
    private volatile byte[] sessionPassword = null;

    private final AtomicReference<ZooKeeper> zooKeeper = new AtomicReference<ZooKeeper>(null);
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final String connectString;
    private final int sessionTimeout;
    private final int connectionTimeoutMs;
    private final AtomicReference<LoggingDriver> log;
    private final AtomicReference<TracerDriver> tracer;
    private final AtomicReference<Watcher> parentWatcher = new AtomicReference<Watcher>(null);
    private final Queue<Exception> backgroundExceptions = new ConcurrentLinkedQueue<Exception>();

    private static final int        MAX_BACKGROUND_EXCEPTIONS = 10;

    ConnectionState(String connectString, int sessionTimeoutMs, int connectionTimeoutMs, Watcher parentWatcher, AtomicReference<LoggingDriver> log, AtomicReference<TracerDriver> tracer) throws IOException
    {
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.log = log;
        this.tracer = tracer;
        this.parentWatcher.set(parentWatcher);
    }

    ZooKeeper getZooKeeper() throws Exception
    {
        Exception exception = backgroundExceptions.poll();
        if ( exception != null )
        {
            log.get().error("Background exception caught", exception);
            throw exception;
        }

        boolean localIsConnected = isConnected.get();
        if ( !localIsConnected )
        {
            long        elapsed = System.currentTimeMillis() - connectionStartMs;
            if ( elapsed >= connectionTimeoutMs )
            {
                KeeperException.ConnectionLossException connectionLossException = new KeeperException.ConnectionLossException();
                log.get().error("Connection timed out", connectionLossException);
                throw connectionLossException;
            }
        }

        return zooKeeper.get();
    }

    boolean isConnected()
    {
        return isConnected.get();
    }

    Watcher substituteParentWatcher(Watcher newWatcher)
    {
        return parentWatcher.getAndSet(newWatcher);
    }

    void        start() throws IOException
    {
        log.get().debug("Starting");
        reset();
    }

    @Override
    public void        close()
    {
        log.get().debug("Closing");

        ZooKeeper localZooKeeper = zooKeeper.getAndSet(null);
        internalClose(localZooKeeper);
    }

    private void internalClose(ZooKeeper localZooKeeper)
    {
        if ( localZooKeeper != null )
        {
            try
            {
                localZooKeeper.close();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                // Interrupted trying to close Zookeeper. Ignoring at this level.
            }
        }
        isConnected.set(false);
    }

    private void reset() throws IOException
    {
        ZooKeeper localZooKeeper;
        if ( sessionId == 0 )
        {
            // i.e. first time connecting
            localZooKeeper = new ZooKeeper(connectString, sessionTimeout, this);
        }
        else
        {
            localZooKeeper = new ZooKeeper(connectString, sessionTimeout, this, sessionId, sessionPassword);
        }
        ZooKeeper oldZooKeeper = zooKeeper.getAndSet(localZooKeeper);
        internalClose(oldZooKeeper);

        connectionStartMs = System.currentTimeMillis();
    }

    @Override
    public void process(WatchedEvent event)
    {
        boolean wasConnected = isConnected.get();
        boolean newIsConnected = wasConnected;
        if ( event.getType() == Watcher.Event.EventType.None )
        {
            newIsConnected = (event.getState() == Event.KeeperState.SyncConnected);
            if ( event.getState() == Event.KeeperState.Expired )
            {
                log.get().warn("Session expired event received");
                try
                {
                    reset();
                }
                catch ( IOException e )
                {
                    queueBackgroundException(e);
                }
            }
        }

        if ( newIsConnected != wasConnected )
        {
            isConnected.set(newIsConnected);
            if ( newIsConnected )
            {
                sessionId = zooKeeper.get().getSessionId();
                sessionPassword = zooKeeper.get().getSessionPasswd();
            }
        }

        Watcher localParentWatcher = parentWatcher.get();
        if ( localParentWatcher != null )
        {
            TimeTrace timeTrace = new TimeTrace("connection-state-parent-process", tracer.get());
            localParentWatcher.process(event);
            timeTrace.commit();
        }
    }

    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored"})
    private void queueBackgroundException(IOException e)
    {
        while ( backgroundExceptions.size() >= MAX_BACKGROUND_EXCEPTIONS )
        {
            backgroundExceptions.poll();
        }
        backgroundExceptions.offer(e);
    }
}
