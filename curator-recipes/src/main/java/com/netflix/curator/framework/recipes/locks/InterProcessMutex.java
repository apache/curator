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

import com.netflix.curator.framework.CuratorFramework;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A re-entrant mutex that works across JVMs. Uses Zookeeper to hold the lock. All processes in all JVMs that
 * use the same lock path will achieve an inter-process critical section. Further, this mutex is
 * "fair" - each user will get the mutex in the order requested (from ZK's point of view)
 */
public class InterProcessMutex implements InterProcessLock
{
    private final LockInternals<?> internals;
    private final String                basePath;

    private volatile LockData           lockData;

    private static final SharedCount           sharedCount = new SharedCount()
    {
        @Override
        public int getCount() throws Exception
        {
            return 1;
        }
    };

    private static class LockData
    {
        volatile Thread     owningThread;
        volatile String     lockPath;
        volatile int        lockCount;
    }

    private static final String LOCK_NAME = "lock-";

    /**
     * @param client client
     * @param path the path to lock
     */
    public InterProcessMutex(CuratorFramework client, String path)
    {
        this(client, path, null);
    }

    /**
     * @param client client
     * @param path the path to lock
     * @param clientClosingListener if not null, will get called if client connection unexpectedly closes
     */
    public InterProcessMutex(CuratorFramework client, String path, ClientClosingListener<InterProcessMutex> clientClosingListener)
    {
        basePath = path;
        internals = new LockInternals<InterProcessMutex>(client, path, LOCK_NAME, this, clientClosingListener);
    }

    /**
     * Acquire the mutex - blocking until it's available. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire must be balanced by a call
     * to {@link #release()}
     *
     * @throws Exception ZK errors, interruptions, another thread owns the lock
     */
    @Override
    public void acquire() throws Exception
    {
        if ( !internalLock(-1, null) )
        {
            throw new IOException("Lost connection while trying to acquire lock: " + basePath);
        }
    }

    /**
     * Acquire the mutex - blocks until it's available or the given time expires. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire that returns true must be balanced by a call
     * to {@link #release()}
     *
     * @param time time to wait
     * @param unit time unit
     * @return true if the mutex was acquired, false if not
     * @throws Exception ZK errors, interruptions, another thread owns the lock
     */
    @Override
    public boolean acquire(long time, TimeUnit unit) throws Exception
    {
        return internalLock(time, unit);
    }

    /**
     * Returns true if the mutex is acquired by a thread in this JVM
     *
     * @return true/false
     */
    @Override
    public boolean isAcquiredInThisProcess()
    {
        return lockData != null;
    }

    /**
     * Perform one release of the mutex if the calling thread is the same thread that acquired it. If the
     * thread had made multiple calls to acquire, the mutex will still be held when this method returns.
     *
     * @throws Exception ZK errors, interruptions, current thread does not own the lock
     */
    @Override
    public void release() throws Exception
    {
        LockData    localData = lockData;
        if ( (localData == null) || (localData.owningThread != Thread.currentThread()) )
        {
            throw new IllegalMonitorStateException("You do not own the lock: " + basePath);
        }

        if ( --localData.lockCount > 0 )
        {
            return;
        }
        if ( localData.lockCount < 0 )
        {
            throw new IllegalMonitorStateException("Lock count has gone negative for lock: " + basePath);
        }

        internals.releaseLock(localData.lockPath);
        lockData = null;
    }

    private boolean internalLock(long time, TimeUnit unit) throws Exception
    {
        LockData    localData = lockData;
        boolean     re_entering = (localData != null) && (localData.owningThread == Thread.currentThread());
        if ( !re_entering && (localData != null) )
        {
            throw new IllegalMonitorStateException("This lock: " + basePath + " is already owned by " + localData.owningThread);
        }

        if ( re_entering )
        {
            ++localData.lockCount;
            return true;
        }

        String lockPath = internals.attemptLock(time, unit, sharedCount);
        if ( lockPath != null )
        {
            LockData        localLockData = new LockData();
            localLockData.lockPath = lockPath;
            localLockData.lockCount = 1;
            localLockData.owningThread = Thread.currentThread();

            lockData = localLockData;
            return true;
        }

        return false;
    }
}
