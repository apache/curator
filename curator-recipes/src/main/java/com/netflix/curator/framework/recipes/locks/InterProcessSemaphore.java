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
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.nio.ByteBuffer;
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
 *     The maximum number of leases is stored in a common node in ZK. Each unique InterProcessSemaphore
 *     path will have a single maximum leases node. Initially, the maximum is <code>0</code>. Use
 *     either {@link #setMaxLeases(int)} or {@link #compareAndSetMaxLeases(int, int)} to adjust
 *     the maximum number of leases as needed. Semaphores that share the path will adjust to
 *     the maximum number of leases via watches.
 * </p>
 *
 * <p>
 *     For a given path, no more than the currently set maximum number leases will be allowed. Note though
 *     that ZooKeeper's consistency guarantees apply and so it is possible that more than maximum
 *     leases can be acquired by clients that have yet to receive an updated value for the maximum
 *     lease node.
 * </p>
 *
 * <p>
 *     The various acquire methods return {@link Lease} objects that represent acquired leases. Clients
 *     must take care to close lease objects else the lease will be lost (ideally in a <code>finally</code>
 *     block). However, if the client session drops (crash, etc.), any leases held by the client are
 *     automatically closed and made available to other clients.
 * </p>
 */
public class InterProcessSemaphore
{
    private final CuratorFramework  client;
    private final LockInternals<?>  internals;
    private final String            maxLeasesPath;
    private final EnsurePath        ensureMaxLeasesPath;
    private final Watcher           watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            internals.getSyncLock().lock();
            try
            {
                maxLeases = DIRTY;
                internals.getSyncCondition().signalAll();
            }
            finally
            {
                internals.getSyncLock().unlock();
            }
        }
    };
    private final SharedCount               sharedCount = new SharedCount()
    {
        @Override
        public int getCount() throws Exception
        {
            checkMaxLeases();
            return maxLeases;
        }
    };

    private volatile int            maxLeases = DIRTY;
    private volatile Stat           maxLeasesStat = new Stat();

    private static final int            DIRTY = -1;

    private static final String         LOCK_PATH = "locks";
    private static final String         MAX_LEASES_PATH = "max-leases";

    /**
     * @param client the client
     * @param path path for the semaphore
     */
    public InterProcessSemaphore(CuratorFramework client, String path)
    {
        this(client, path, null);
    }

    /**
     * @param client the client
     * @param path path for the semaphore
     * @param clientClosingListener if not null, will get called if client connection unexpectedly closes
     */
    public InterProcessSemaphore(CuratorFramework client, String path, ClientClosingListener<InterProcessSemaphore> clientClosingListener)
    {
        this.client = client;
        
        maxLeasesPath = ZKPaths.makePath(path, MAX_LEASES_PATH);
        internals = new LockInternals<InterProcessSemaphore>(client, path, LOCK_PATH, this, clientClosingListener);
        ensureMaxLeasesPath = client.newNamespaceAwareEnsurePath(path);
    }

    /**
     * Atomically set a new value for the maximum number of leases. The new value is set only if
     * the current value is the same as the <code>expectedValue</code> parameter. All semaphores
     * that use this instance's path will update to use the new maximum number of leases value (ZK's
     * normally consistency rules apply).
     * 
     * @param expectedValue the value currently expected for max leases. If the current value
     * doesn't match, the max leases is not changed
     * @param newValue new value for maximum leases
     * @return true if the new value was set, false if not
     * @throws Exception ZK errors, interruptions, etc.
     */
    public boolean  compareAndSetMaxLeases(int expectedValue, int newValue) throws Exception
    {
        checkMaxLeases();
        
        if ( expectedValue != maxLeases )
        {
            return false;
        }

        try
        {
            ensureMaxLeasesPath.ensure(client.getZookeeperClient());
            client.setData().withVersion(maxLeasesStat.getVersion()).forPath(maxLeasesPath, maxToBytes(newValue));
        }
        catch ( KeeperException.BadVersionException dummy )
        {
            return false;
        }

        return true;
    }

    /**
     * Atomically set a new value for the maximum number of leases irrespective of its previous value.
     * All semaphores that use this instance's path will update to use the new maximum number of leases
     * value (ZK's normally consistency rules apply).
     *
     * @param newValue new value for maximum leases
     * @throws Exception ZK errors, interruptions, etc.
     */
    public void     setMaxLeases(int newValue) throws Exception
    {
        checkMaxLeases();

        client.setData().forPath(maxLeasesPath, maxToBytes(newValue));
    }

    /**
     * Convenience method. Closes all leases in the given collection of leases
     *
     * @param leases leases to close
     */
    public void     closeAll(Collection<Lease> leases)
    {
        for ( Lease l : leases )
        {
            Closeables.closeQuietly(l);
        }
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
        String      path = internalAcquire(-1, null);
        return makeLease(path);
    }

    /**
     * <p>Acquire <code>qty</code> leases. If there are not enough leases available, this method
     * blocks until either the maximum number of leases is increased enough or other clients/processes
     * close enough leases.</p>
     *
     * <p>The client must close the leases when it is done with them. You should do this in a
     * <code>finally</code> block. NOTE: You can use {@link #closeAll(Collection)} for this.</p>
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
                String      path = internalAcquire(-1, null);
                builder.add(makeLease(path));
            }
        }
        catch ( Exception e )
        {
            closeAll(builder.build());
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
        String      path = internalAcquire(time, unit);
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
     * <code>finally</code> block. NOTE: You can use {@link #closeAll(Collection)} for this.</p>
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

                String      path = (thisWaitMs > 0) ? internalAcquire(thisWaitMs, TimeUnit.MILLISECONDS) : null;
                if ( path == null )
                {
                    closeAll(builder.build());
                    return null;
                }
                builder.add(makeLease(path));
            }
        }
        catch ( Exception e )
        {
            closeAll(builder.build());
            throw e;
        }

        return builder.build();
    }

    private String internalAcquire(long thisWaitMs, TimeUnit unit) throws Exception
    {
        internals.getSyncLock().lock();
        try
        {
            return internals.attemptLock(thisWaitMs, unit, sharedCount);
        }
        finally
        {
            internals.getSyncLock().unlock();
        }
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

    private static byte[] maxToBytes(int max)
    {
        byte[]      bytes = new byte[4];
        ByteBuffer.wrap(bytes).putInt(max);

        return bytes;
    }

    private int     readMaxLeases() throws Exception
    {
        byte[]          bytes = null;
        try
        {
            Stat        newStat = new Stat();
            bytes = client.getData().storingStatIn(newStat).usingWatcher(watcher).forPath(maxLeasesPath);
            maxLeasesStat = newStat;
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            // ignore
        }

        if ( bytes == null )
        {
            try
            {
                client.create().forPath(maxLeasesPath, maxToBytes(0));
            }
            catch ( KeeperException.NodeExistsException ignore )
            {
                // ignore
            }
            return readMaxLeases(); // have to re-read to set our watcher
        }
        return ByteBuffer.wrap(bytes).getInt();
    }

    private void checkMaxLeases() throws Exception
    {
        ensureMaxLeasesPath.ensure(client.getZookeeperClient());

        internals.getSyncLock().lock();
        try
        {
            if ( maxLeases == DIRTY )
            {
                maxLeases = readMaxLeases();
                internals.getSyncCondition().signalAll();
            }
        }
        finally
        {
            internals.getSyncLock().unlock();
        }
    }
}
