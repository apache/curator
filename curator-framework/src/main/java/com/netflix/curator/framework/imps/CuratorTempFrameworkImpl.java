package com.netflix.curator.framework.imps;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.CuratorTempFramework;
import com.netflix.curator.framework.api.TempGetDataBuilder;
import com.netflix.curator.framework.api.transaction.CuratorTransaction;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class CuratorTempFrameworkImpl implements CuratorTempFramework
{
    private final CuratorFrameworkFactory.Builder   factory;
    private final long                              inactiveThresholdMs;

    // guarded by sync
    private CuratorFrameworkImpl                    client;

    // guarded by sync
    private ScheduledExecutorService                cleanup;

    // guarded by sync
    private long                                    lastAccess;

    public CuratorTempFrameworkImpl(CuratorFrameworkFactory.Builder factory, long inactiveThresholdMs)
    {
        this.factory = factory;
        this.inactiveThresholdMs = inactiveThresholdMs;
    }

    @Override
    public void close()
    {
        closeClient();
    }

    @Override
    public CuratorTransaction inTransaction() throws Exception
    {
        openConnectionIfNeeded();
        return new CuratorTransactionImpl(client);
    }

    @Override
    public TempGetDataBuilder getData() throws Exception
    {
        openConnectionIfNeeded();
        return new TempGetDataBuilderImpl(client);
    }

    @VisibleForTesting
    CuratorFrameworkImpl getClient()
    {
        return client;
    }

    @VisibleForTesting
    ScheduledExecutorService getCleanup()
    {
        return cleanup;
    }

    @VisibleForTesting
    synchronized void updateLastAccess()
    {
        lastAccess = System.currentTimeMillis();
    }

    private synchronized void openConnectionIfNeeded() throws Exception
    {
        if ( client == null )
        {
            client = (CuratorFrameworkImpl)factory.build(); // cast is safe - we control both sides of this
            client.start();
        }

        if ( cleanup == null )
        {
            ThreadFactory threadFactory = factory.getThreadFactory();
            cleanup = (threadFactory != null) ? Executors.newScheduledThreadPool(1, threadFactory) : Executors.newScheduledThreadPool(1);

            Runnable        command = new Runnable()
            {
                @Override
                public void run()
                {
                    checkInactive();
                }
            };
            cleanup.scheduleAtFixedRate(command, inactiveThresholdMs, inactiveThresholdMs, TimeUnit.MILLISECONDS);
        }

        updateLastAccess();
    }

    private synchronized void checkInactive()
    {
        long        elapsed = System.currentTimeMillis() - lastAccess;
        if ( elapsed >= inactiveThresholdMs )
        {
            closeClient();
        }
    }

    private synchronized void closeClient()
    {
        if ( cleanup != null )
        {
            cleanup.shutdownNow();
            cleanup = null;
        }

        if ( client != null )
        {
            Closeables.closeQuietly(client);
            client = null;
        }
    }
}
