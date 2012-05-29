package com.netflix.curator.framework.recipes.locks;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.Timing;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

class SemaphoreClient implements Callable<Void>,ConnectionStateListener
{
    private final CuratorFramework client;
    private final String semaphorePath;
    private final Callable<Void> operation;

    private volatile boolean shouldRun;
    private volatile boolean hasAcquired;

    private static final int CLIENT_EXCEPTION_HANDLER_SLEEP_TIME_SECS = 10;
    private static final int MAX_SEMAPHORE_LEASES = 1;

    private static final AtomicReference<SemaphoreClient>       activeClient = new AtomicReference<SemaphoreClient>(null);

    SemaphoreClient(String connectionString, String semaphorePath, Callable<Void> operation) throws IOException
    {
        Timing timing = new Timing();
        this.client = CuratorFrameworkFactory.newClient(connectionString, timing.session(), timing.connection(), new ExponentialBackoffRetry(100, 3));
        client.start();

        this.semaphorePath = semaphorePath;
        this.operation = operation;
    }

    void stop()
    {
        shouldRun = false;
    }

    boolean hasAcquired()
    {
        return hasAcquired;
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        hasAcquired = false;
    }

    static SemaphoreClient getActiveClient()
    {
        return activeClient.get();
    }

    @Override
    public Void call() throws Exception
    {
        shouldRun = true;
        client.getConnectionStateListenable().addListener(this);
        try
        {
            while ( shouldRun )
            {
                try
                {
                    acquireAndRun();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    // propagate up, don't sleep
                    throw e;

                }
                catch ( Exception e )
                {
                    Thread.sleep(CLIENT_EXCEPTION_HANDLER_SLEEP_TIME_SECS * 1000L);
                }
            }

        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
        return null;
    }

    private void acquireAndRun() throws Exception
    {
        InterProcessSemaphore semaphore = new InterProcessSemaphore(client, semaphorePath, MAX_SEMAPHORE_LEASES);
        Lease lease = semaphore.acquire();
        try
        {
            hasAcquired = true;
            if ( activeClient.compareAndSet(null, this) )
            {
                throw new Exception("Multiple acquirers");
            }

            try
            {
                while ( hasAcquired && shouldRun )
                {
                    operation.call();
                }
            }
            finally
            {
                if ( activeClient.compareAndSet(this, null) )
                {
                    //noinspection ThrowFromFinallyBlock
                    throw new Exception("Bad release");
                }
            }
        }
        finally
        {
            semaphore.returnLease(lease);
        }
    }
}
