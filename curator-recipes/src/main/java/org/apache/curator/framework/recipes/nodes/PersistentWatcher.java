package org.apache.curator.framework.recipes.nodes;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class PersistentWatcher implements Closeable
{
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final ListenerContainer<Watcher> listeners = new ListenerContainer<>();
    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            if ( newState.isConnected() )
            {
                reset();
            }
        }
    };
    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(final WatchedEvent event)
        {
            Function<Watcher, Void> function = new Function<Watcher, Void>()
            {
                @Override
                public Void apply(Watcher watcher)
                {
                    watcher.process(event);
                    return null;
                }
            };
            listeners.forEach(function);
        }
    };
    private final CuratorFramework client;
    private final String basePath;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public PersistentWatcher(CuratorFramework client, String basePath)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.basePath = Objects.requireNonNull(basePath, "basePath cannot be null");
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");
        client.getConnectionStateListenable().addListener(connectionStateListener);
        reset();
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            client.getConnectionStateListenable().removeListener(connectionStateListener);
            try
            {
                client.watches().remove(watcher).inBackground().forPath(basePath);
            }
            catch ( Exception e )
            {
                // TODO
            }
        }
    }

    public Listenable<Watcher> getListenable()
    {
        return listeners;
    }

    protected void reset()
    {
        try
        {
            client.addPersistentWatch().inBackground().usingWatcher(watcher).forPath(basePath);
        }
        catch ( Exception e )
        {
            // TODO
        }
    }
}
