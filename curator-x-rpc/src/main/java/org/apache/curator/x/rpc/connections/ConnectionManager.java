package org.apache.curator.x.rpc.connections;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.x.rpc.configuration.ConnectionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectionManager implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Cache<String, CuratorEntry> cache;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final Map<String, ConnectionConfiguration> connections;
    private final ScheduledExecutorService service = ThreadUtils.newSingleThreadScheduledExecutor("ConnectionManager");

    private static final int FORCED_CLEANUP_SECONDS = 30;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    public ConnectionManager(List<ConnectionConfiguration> connections, long expirationMs)
    {
        this.connections = buildConnectionsMap(connections);

        RemovalListener<String, CuratorEntry> listener = new RemovalListener<String, CuratorEntry>()
        {
            @SuppressWarnings("NullableProblems")
            @Override
            public void onRemoval(RemovalNotification<String, CuratorEntry> notification)
            {
                if ( notification != null )
                {
                    log.debug(String.format("Entry being removed. id (%s), reason (%s)", notification.getKey(), notification.getCause()));

                    CuratorEntry entry = notification.getValue();
                    if ( entry != null )
                    {
                        entry.close();
                    }
                }
            }
        };
        cache = CacheBuilder.newBuilder().expireAfterAccess(expirationMs, TimeUnit.MILLISECONDS).removalListener(listener).build();
    }

    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        Runnable cleanup = new Runnable()
        {
            @Override
            public void run()
            {
                cache.cleanUp();
            }
        };
        service.scheduleWithFixedDelay(cleanup, FORCED_CLEANUP_SECONDS, 30, TimeUnit.SECONDS);
    }

    @Override
    public void close()
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            service.shutdownNow();
            cache.invalidateAll();
            cache.cleanUp();
        }
    }

    public CuratorFramework newConnection(String name)
    {
        ConnectionConfiguration configuration = connections.get(name);
        return (configuration != null) ? configuration.build() : null;
    }

    public void add(String id, CuratorFramework client)
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");
        cache.put(id, new CuratorEntry(client));
    }

    public CuratorEntry get(String id)
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");
        return cache.getIfPresent(id);
    }

    public CuratorEntry remove(String id)
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");
        return cache.asMap().remove(id);
    }

    private Map<String, ConnectionConfiguration> buildConnectionsMap(List<ConnectionConfiguration> connections)
    {
        Preconditions.checkArgument(connections.size() > 0, "You must have at least one connection configured");

        ImmutableMap.Builder<String, ConnectionConfiguration> builder = ImmutableMap.builder();
        for ( ConnectionConfiguration configuration : connections )
        {
            builder.put(configuration.getName(), configuration);
        }
        return builder.build();
    }
}
