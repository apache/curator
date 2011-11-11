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

public class InterProcessSemaphore
{
    private final CuratorFramework          client;
    private final LockInternals<?> internals;
    private final String                    maxLeasesPath;
    private final EnsurePath                ensureMaxLeasesPath;
    private final Watcher                   watcher = new Watcher()
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

    public InterProcessSemaphore(CuratorFramework client, String path)
    {
        this(client, path, null);
    }

    public InterProcessSemaphore(CuratorFramework client, String path, final ClientClosingListener<InterProcessSemaphore> clientClosingListener)
    {
        this.client = client;
        
        maxLeasesPath = ZKPaths.makePath(path, MAX_LEASES_PATH);
        internals = new LockInternals<InterProcessSemaphore>(client, path, LOCK_PATH, this, clientClosingListener);
        ensureMaxLeasesPath = client.newNamespaceAwareEnsurePath(path);
    }

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

    public void     setMaxLeases(int newValue) throws Exception
    {
        checkMaxLeases();

        client.setData().forPath(maxLeasesPath, maxToBytes(newValue));
    }

    public void     closeAll(Collection<Lease> leases)
    {
        for ( Lease l : leases )
        {
            Closeables.closeQuietly(l);
        }
    }

    public Lease acquire() throws Exception
    {
        String      path = internalAcquire(-1, null);
        return makeLease(path);
    }

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

    public Lease acquire(long time, TimeUnit unit) throws Exception
    {
        String      path = internalAcquire(time, unit);
        return (path != null) ? makeLease(path) : null;
    }

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
