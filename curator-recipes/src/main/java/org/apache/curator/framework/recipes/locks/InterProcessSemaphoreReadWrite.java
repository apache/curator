/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.locks;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.curator.framework.CuratorFramework;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class InterProcessSemaphoreReadWrite implements InterProcessReadWriteLockBase
{
    private final InterProcessSemaphoreV2 lock;
    private final AtomicReference<Lease> writeLease = new AtomicReference<Lease>();
    private final List<Lease> readLeases = new CopyOnWriteArrayList<Lease>();

    private static final LockInternalsSorter sorter = new LockInternalsSorter()
    {
        @Override
        public String fixForSorting(String str, String lockName)
        {
            return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
        }
    };

    private final InterProcessLock readLock = new InterProcessLock()
    {
        @Override
        public void acquire() throws Exception
        {
            internalAcquire(-1, null, false);
        }

        @Override
        public boolean acquire(long time, TimeUnit unit) throws Exception
        {
            return internalAcquire(time, unit, false);
        }

        @Override
        public void release() throws Exception
        {
            internalRelease(false);
        }

        @Override
        public Collection<String> getParticipantNodes() throws Exception
        {
            return Collections2.filter(lock.getParticipantNodes(), new Predicate<String>()
            {
                @Override
                public boolean apply(String name)
                {
                    return name.contains(READ_BASE_NAME);
                }
            });
        }

        @Override
        public boolean isAcquiredInThisProcess()
        {
            return readLeases.size() > 0;
        }
    };
    private final InterProcessLock writeLock = new InterProcessLock()
    {
        @Override
        public void acquire() throws Exception
        {
            internalAcquire(-1, null, true);
        }

        @Override
        public boolean acquire(long time, TimeUnit unit) throws Exception
        {
            return internalAcquire(time, unit, true);
        }

        @Override
        public void release() throws Exception
        {
            internalRelease(true);
        }

        @Override
        public Collection<String> getParticipantNodes() throws Exception
        {
            return Collections2.filter(lock.getParticipantNodes(), new Predicate<String>()
            {
                @Override
                public boolean apply(String name)
                {
                    return name.contains(WRITE_BASE_NAME);
                }
            });
        }

        @Override
        public boolean isAcquiredInThisProcess()
        {
            return writeLease.get() != null;
        }
    };

    private static final String SUFFIX = "-lease-";
    private static final String READ_BASE_NAME = "read" + SUFFIX;
    private static final String WRITE_BASE_NAME = "write" + SUFFIX;

    public InterProcessSemaphoreReadWrite(CuratorFramework client, String path)
    {
        lock = new InterProcessSemaphoreV2(client, path, Integer.MAX_VALUE);
    }

    @Override
    public InterProcessLock     readLock()
    {
        return readLock;
    }

    @Override
    public InterProcessLock     writeLock()
    {
        return writeLock;
    }

    private boolean internalShouldGetLease(String ourNodeName, List<String> children, boolean isWriter)
    {
        Preconditions.checkArgument(children.size() > 0, "Empty children list");

        children = LockInternals.getSortedChildren(SUFFIX, sorter, children);
        String firstNodeName = children.get(0);

        if ( isWriter )
        {
            return firstNodeName.equals(ourNodeName);
        }

        return !firstNodeName.contains(WRITE_BASE_NAME);
    }

    private boolean internalAcquire(long time, TimeUnit unit, final boolean isWriter) throws Exception
    {
        Preconditions.checkState(!isWriter || (writeLease.get() == null), "Write lock already held by this InterProcessSemaphoreReadWrite");

        InterProcessSemaphoreV2.LeaseAcquirePredicate acquireFilter = new InterProcessSemaphoreV2.LeaseAcquirePredicate()
        {
            @Override
            public boolean shouldGetLease(String ourNodeName, List<String> children, int maxLeases)
            {
                return internalShouldGetLease(ourNodeName, children, isWriter);
            }
        };
        Collection<Lease> leases = lock.internalAcquire(1, time, unit, isWriter ? WRITE_BASE_NAME : READ_BASE_NAME, acquireFilter);
        if ( leases.size() == 0 )
        {
            return false;
        }

        Lease lease = leases.iterator().next();
        if ( isWriter )
        {
            writeLease.set(lease);
        }
        else
        {
            readLeases.add(lease);
        }

        return true;
    }

    private void internalRelease(boolean isWriter)
    {
        Lease lease;
        if ( isWriter )
        {
            Preconditions.checkState(writeLease.get() != null, "Write lock not held by this InterProcessSemaphoreReadWrite");
            lease = writeLease.getAndSet(null);
        }
        else
        {
            Preconditions.checkState(readLeases.size() > 0, "A read lock is not held by this InterProcessSemaphoreReadWrite");
            lease = readLeases.remove(0);
        }
        lock.returnLease(lease);
    }
}
