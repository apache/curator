/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.curator;

import com.google.common.io.Closeables;
import com.netflix.curator.drivers.TracerDriver;
import com.netflix.curator.ensemble.EnsembleProvider;
import com.netflix.curator.utils.DebugUtils;
import com.netflix.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class ConnectionState implements Watcher, Closeable
{
    private volatile long connectionStartMs = 0;

    private final Logger                        log = LoggerFactory.getLogger(getClass());
    private final HandleHolder                  zooKeeper;
    private final AtomicBoolean                 isConnected = new AtomicBoolean(false);
    private final AtomicBoolean                 lost = new AtomicBoolean(false);
    private final EnsembleProvider              ensembleProvider;
    private final int                           connectionTimeoutMs;
    private final AtomicReference<TracerDriver> tracer;
    private final Queue<Exception>              backgroundExceptions = new ConcurrentLinkedQueue<Exception>();
    private final Queue<Watcher>                parentWatchers = new ConcurrentLinkedQueue<Watcher>();

    private static final int        MAX_BACKGROUND_EXCEPTIONS = 10;
    private static final boolean    LOG_EVENTS = Boolean.getBoolean(DebugUtils.PROPERTY_LOG_EVENTS);

    ConnectionState(ZookeeperFactory zookeeperFactory, EnsembleProvider ensembleProvider, int sessionTimeoutMs, int connectionTimeoutMs, Watcher parentWatcher, AtomicReference<TracerDriver> tracer, boolean canBeReadOnly)
    {
        this.ensembleProvider = ensembleProvider;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.tracer = tracer;
        if ( parentWatcher != null )
        {
            parentWatchers.offer(parentWatcher);
        }

        zooKeeper = new HandleHolder(zookeeperFactory, this, ensembleProvider, sessionTimeoutMs, canBeReadOnly);
    }

    ZooKeeper getZooKeeper() throws Exception
    {
        if ( SessionFailRetryLoop.sessionForThreadHasFailed() )
        {
            throw new SessionFailRetryLoop.SessionFailedException();
        }

        if ( lost.compareAndSet(true, false) )
        {
            log.debug("resetting after loss");
            reset();
        }

        Exception exception = backgroundExceptions.poll();
        if ( exception != null )
        {
            log.error("Background exception caught", exception);
            tracer.get().addCount("background-exceptions", 1);
            throw exception;
        }

        boolean localIsConnected = isConnected.get();
        if ( !localIsConnected )
        {
            long        elapsed = System.currentTimeMillis() - connectionStartMs;
            if ( elapsed >= connectionTimeoutMs )
            {
                if ( zooKeeper.hasNewConnectionString() )
                {
                    handleNewConnectionString();
                }
                else
                {
                    KeeperException.ConnectionLossException connectionLossException = new KeeperException.ConnectionLossException();
                    if ( !Boolean.getBoolean(DebugUtils.PROPERTY_DONT_LOG_CONNECTION_ISSUES) )
                    {
                        log.error(String.format("Connection timed out for connection string (%s) and timeout (%d) / elapsed (%d)", zooKeeper.getConnectionString(), connectionTimeoutMs, elapsed), connectionLossException);
                    }
                    tracer.get().addCount("connections-timed-out", 1);
                    throw connectionLossException;
                }
            }
        }

        return zooKeeper.getZooKeeper();
    }

    boolean isConnected()
    {
        return isConnected.get();
    }

    void        start() throws Exception
    {
        log.debug("Starting");
        ensembleProvider.start();
        reset();
    }

    @Override
    public void        close() throws IOException
    {
        log.debug("Closing");

        Closeables.closeQuietly(ensembleProvider);
        try
        {
            zooKeeper.closeAndClear();
        }
        catch ( Exception e )
        {
            throw new IOException(e);
        }
        finally
        {
            isConnected.set(false);
            lost.set(false);
        }
    }

    void        addParentWatcher(Watcher watcher)
    {
        parentWatchers.offer(watcher);
    }

    void        removeParentWatcher(Watcher watcher)
    {
        parentWatchers.remove(watcher);
    }

    void markLost()
    {
        log.debug("lost");

        lost.set(true);
    }

    @Override
    public void process(WatchedEvent event)
    {
        if ( LOG_EVENTS )
        {
            log.debug("ConnectState watcher: " + event);
        }

        for ( Watcher parentWatcher : parentWatchers )
        {
            TimeTrace timeTrace = new TimeTrace("connection-state-parent-process", tracer.get());
            parentWatcher.process(event);
            timeTrace.commit();
        }

        boolean wasConnected = isConnected.get();
        boolean newIsConnected = wasConnected;
        if ( event.getType() == Watcher.Event.EventType.None )
        {
            newIsConnected = checkState(event.getState(), wasConnected);
        }

        if ( newIsConnected != wasConnected )
        {
            isConnected.set(newIsConnected);
            if ( newIsConnected )
            {
                lost.set(false);
            }
            connectionStartMs = System.currentTimeMillis();
        }
    }

    EnsembleProvider getEnsembleProvider()
    {
        return ensembleProvider;
    }

    private synchronized void reset() throws Exception
    {
        log.debug("reset");

        isConnected.set(false);
        connectionStartMs = System.currentTimeMillis();
        zooKeeper.closeAndReset();
        zooKeeper.getZooKeeper();   // initiate connection
    }

    private boolean checkState(Event.KeeperState state, boolean wasConnected)
    {
        boolean     isConnected = wasConnected;
        boolean     checkNewConnectionString = true;
        switch ( state )
        {
            default:
            case Disconnected:
            {
                isConnected = false;
                break;
            }

            case SyncConnected:
            case ConnectedReadOnly:
            {
                isConnected = true;
                break;
            }

            case AuthFailed:
            {
                isConnected = false;
                log.error("Authentication failed");
                break;
            }

            case Expired:
            {
                isConnected = false;
                checkNewConnectionString = false;
                handleExpiredSession();
                break;
            }

            case SaslAuthenticated:
            {
                // NOP
                break;
            }
        }

        if ( checkNewConnectionString && zooKeeper.hasNewConnectionString() )
        {
            handleNewConnectionString();
        }

        return isConnected;
    }

    private void handleNewConnectionString()
    {
        log.info("Connection string changed");
        tracer.get().addCount("connection-string-changed", 1);

        try
        {
            reset();
        }
        catch ( Exception e )
        {
            queueBackgroundException(e);
        }
    }

    private void handleExpiredSession()
    {
        log.warn("Session expired event received");
        tracer.get().addCount("session-expired", 1);

        try
        {
            reset();
        }
        catch ( Exception e )
        {
            queueBackgroundException(e);
        }
    }

    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored"})
    private void queueBackgroundException(Exception e)
    {
        while ( backgroundExceptions.size() >= MAX_BACKGROUND_EXCEPTIONS )
        {
            backgroundExceptions.poll();
        }
        backgroundExceptions.offer(e);
    }
}
