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
package com.netflix.curator.framework.recipes.locks;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.shared.SharedCountListener;
import com.netflix.curator.framework.recipes.shared.SharedCountReader;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     A counting semaphore that works across JVMs. All processes
 *     in all JVMs that use the same lock path will achieve an inter-process limited set of leases.
 *     Further, this semaphore is mostly "fair" - each user will get a lease in the order requested
 *     (from ZK's point of view).
 * </p>
 *
 * <p>
 *     There are two modes for determining the max leases for the semaphore. In the first mode the
 *     max leases is a convention maintained by the users of a given path. In the second mode a
 *     {@link SharedCountReader} is used as the method for semaphores of a given path to determine
 *     the max leases.
 * </p>
 *
 * <p>
 *     If a {@link SharedCountReader} is <b>not</b> used, no internal checks are done to prevent
 *     Process A acting as if there are 10 leases and Process B acting as if there are 20. Therefore,
 *     make sure that all instances in all processes use the same numberOfLeases value.
 * </p>
 *
 * <p>
 *     The various acquire methods return {@link Lease} objects that represent acquired leases. Clients
 *     must take care to close lease objects  (ideally in a <code>finally</code>
 *     block) else the lease will be lost. However, if the client session drops (crash, etc.),
 *     any leases held by the client are
 *     automatically closed and made available to other clients.
 * </p>
 */
public class InterProcessSemaphore
{
    private final LockInternals<?>  internals;

    private static final String     LOCK_NAME = "lock-";

    /**
     * @param client the client
     * @param path path for the semaphore
     * @param maxLeases the max number of leases to allow for this instance
     */
    public InterProcessSemaphore(CuratorFramework client, String path, int maxLeases)
    {
        this(client, path, null, maxLeases, null);
    }

    /**
     * @param client the client
     * @param path path for the semaphore
     * @param count the shared count to use for the max leases
     */
    public InterProcessSemaphore(CuratorFramework client, String path, SharedCountReader count)
    {
        this(client, path, null, 0, count);
    }

    /**
     * @param client the client
     * @param path path for the semaphore
     * @param clientClosingListener if not null, will get called if client connection unexpectedly closes
     * @param maxLeases the max number of leases to allow for this instance
     */
    public InterProcessSemaphore(CuratorFramework client, String path, ClientClosingListener<InterProcessSemaphore> clientClosingListener, int maxLeases)
    {
        this(client, path, clientClosingListener, maxLeases, null);
    }

    /**
     * @param client the client
     * @param path path for the semaphore
     * @param clientClosingListener if not null, will get called if client connection unexpectedly closes
     * @param count the shared count to use for the max leases
     */
    public InterProcessSemaphore(CuratorFramework client, String path, ClientClosingListener<InterProcessSemaphore> clientClosingListener, SharedCountReader count)
    {
        this(client, path, clientClosingListener, 0, count);
    }

    private InterProcessSemaphore(CuratorFramework client, String path, ClientClosingListener<InterProcessSemaphore> clientClosingListener, int maxLeases, SharedCountReader count)
    {
        internals = new LockInternals<InterProcessSemaphore>(client, path, LOCK_NAME, this, clientClosingListener, (count != null) ? count.getCount() : maxLeases);
        if ( count != null )
        {
            count.addListener
            (
                new SharedCountListener()
                {
                    @Override
                    public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception
                    {
                        internals.setMaxLeases(newCount);
                    }
                }
            );
        }
    }

    /**
     * Convenience method. Closes all leases in the given collection of leases
     *
     * @param leases leases to close
     */
    public void     returnAll(Collection<Lease> leases)
    {
        for ( Lease l : leases )
        {
            Closeables.closeQuietly(l);
        }
    }

    /**
     * Convenience method. Closes the lease
     *
     * @param lease lease to close
     */
    public void     returnLease(Lease lease)
    {
        Closeables.closeQuietly(lease);
    }

    /**
     * <p>Acquire a lease. If no leases are available, this method blocks until either the maximum
     * number of leases is increased or another client/process closes a lease.</p>
     *
     * <p>The client must close the lease when it is done with it. You should do this in a
     * <code>finally</code> block.</p>
     *
     * @return the new lease
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Lease acquire() throws Exception
    {
        String      path = internals.attemptLock(-1, null, new byte[0]);
        return makeLease(path);
    }

    /**
     * <p>Acquire <code>qty</code> leases. If there are not enough leases available, this method
     * blocks until either the maximum number of leases is increased enough or other clients/processes
     * close enough leases.</p>
     *
     * <p>The client must close the leases when it is done with them. You should do this in a
     * <code>finally</code> block. NOTE: You can use {@link #returnAll(Collection)} for this.</p>
     *
     * @param qty number of leases to acquire
     * @return the new leases
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<Lease> acquire(int qty) throws Exception
    {
        Preconditions.checkArgument(qty > 0);

        ImmutableList.Builder<Lease>    builder = ImmutableList.builder();
        try
        {
            while ( qty-- > 0 )
            {
                String      path = internals.attemptLock(-1, null, new byte[0]);
                builder.add(makeLease(path));
            }
        }
        catch ( Exception e )
        {
            returnAll(builder.build());
            throw e;
        }
        return builder.build();
    }

    /**
     * <p>Acquire a lease. If no leases are available, this method blocks until either the maximum
     * number of leases is increased or another client/process closes a lease. However, this method
     * will only block to a maximum of the time parameters given.</p>
     *
     * <p>The client must close the lease when it is done with it. You should do this in a
     * <code>finally</code> block.</p>
     *
     * @param time time to wait
     * @param unit time unit
     * @return the new lease or null if time ran out
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Lease acquire(long time, TimeUnit unit) throws Exception
    {
        String      path = internals.attemptLock(time, unit, new byte[0]);
        return (path != null) ? makeLease(path) : null;
    }

    /**
     * <p>Acquire <code>qty</code> leases. If there are not enough leases available, this method
     * blocks until either the maximum number of leases is increased enough or other clients/processes
     * close enough leases. However, this method will only block to a maximum of the time
     * parameters given. If time expires before all leases are acquired, the subset of acquired
     * leases are automatically closed.</p>
     *
     * <p>The client must close the leases when it is done with them. You should do this in a
     * <code>finally</code> block. NOTE: You can use {@link #returnAll(Collection)} for this.</p>
     *
     * @param qty number of leases to acquire
     * @param time time to wait
     * @param unit time unit
     * @return the new leases or null if time ran out
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<Lease> acquire(int qty, long time, TimeUnit unit) throws Exception
    {
        long                startMs = System.currentTimeMillis();
        long                waitMs = TimeUnit.MILLISECONDS.convert(time, unit);

        Preconditions.checkArgument(qty > 0);

        ImmutableList.Builder<Lease>    builder = ImmutableList.builder();
        try
        {
            while ( qty-- > 0 )
            {
                long        elapsedMs = System.currentTimeMillis() - startMs;
                long        thisWaitMs = waitMs - elapsedMs;

                String      path = (thisWaitMs > 0) ? internals.attemptLock(thisWaitMs, TimeUnit.MILLISECONDS, new byte[0]) : null;
                if ( path == null )
                {
                    returnAll(builder.build());
                    return null;
                }
                builder.add(makeLease(path));
            }
        }
        catch ( Exception e )
        {
            returnAll(builder.build());
            throw e;
        }

        return builder.build();
    }

    private Lease makeLease(final String path)
    {
        return new Lease()
        {
            @Override
            public void close() throws IOException
            {
                try
                {
                    internals.releaseLock(path);
                }
                catch ( Exception e )
                {
                    throw new IOException(e);
                }
            }
        };
    }
}
