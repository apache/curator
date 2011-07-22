/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator.framework.recipes.locks;

import com.netflix.curator.framework.CuratorFramework;
import java.util.concurrent.TimeUnit;

/**
 * <p>A counting semaphore that works across JVMs. Uses Zookeeper to hold the lock. All processes in all JVMs that
 * use the same lock path will achieve an inter-process limited set of leases. Further, this mutex is
 * "fair" - each user will get a lease in the order requested (from ZK's point of view).</p>
 *
 * <p>IMPORTANT: The number of leases in the semaphore is merely a convention maintained by the users
 * of a given path. i.e. no internal checks are done to prevent Process A acting as if there are 10 leases
 * and Process B acting as if there are 20. Therefore, make sure that all instances in all processes
 * use the same <code>numberOfLeases</code> value.</p>
 */
public class InterProcessSemaphore extends LockInternals<InterProcessSemaphore>
{
    private volatile LockData           lockData;

    private static final String LOCK_NAME = "semaphore-";

    private static class LockData
    {
        volatile Thread     owningThread;
        volatile String     lockPath;
    }

    /**
     * @param client client
     * @param path the path to lock
     * @param numberOfLeases the number of leases allowed by this semaphore
     */
    public InterProcessSemaphore(CuratorFramework client, String path, int numberOfLeases)
    {
        this(client, path, numberOfLeases, null);
    }

    /**
     * @param client client
     * @param path the path to lock
     * @param numberOfLeases the number of leases allowed by this semaphore
     * @param clientClosingListener if not null, will get called if client connection unexpectedly closes
     */
    public InterProcessSemaphore(CuratorFramework client, String path, int numberOfLeases, ClientClosingListener<InterProcessSemaphore> clientClosingListener)
    {
        super(client, path, LOCK_NAME, clientClosingListener, numberOfLeases);
    }

    /**
     * Acquire a lease if one is available. Otherwise block until one is.  Each call to acquire must be balanced by a call
     * to {@link #release()}
     *
     * @throws Exception ZK errors, interruptions, another thread owns the lock
     */
    public void acquire() throws Exception
    {
        internalAcquire();
    }

    /**
     * Acquire a lease if one is available - blocks until one is available or the given time expires.
     * Each call to acquire that returns true must be balanced by a call to {@link #release()}
     *
     * @param time time to wait
     * @param unit time unit
     * @return true if a lease was acquired, false if not
     * @throws Exception ZK errors, interruptions, another thread owns the lock
     */
    public boolean acquire(long time, TimeUnit unit) throws Exception
    {
        String      lockPath = internalAcquire(time, unit);
        return (lockPath != null);
    }

    /**
     * Release the lease held by this instance
     *
     * @throws Exception ZK errors, interruptions, current thread does not own the lock
     */
    public void  release() throws Exception
    {
        LockData        localData = lockData;
        if ( (localData == null) || (localData.owningThread != Thread.currentThread()) )
        {
            throw new IllegalMonitorStateException("You do not own the lock: " + getBasePath());
        }

        internalRelease(localData.lockPath);
    }

    protected String internalLock(long time, TimeUnit unit) throws Exception
    {
        LockData localData = lockData;
        if ( localData != null )
        {
            throw new IllegalMonitorStateException("This lock: " + getBasePath() + " is already owned by " + localData.owningThread);
        }

        return super.internalLock(time, unit);
    }

    protected InterProcessSemaphore getInstance()
    {
        return this;
    }

    @Override
    protected void setLockData(String ourPath)
    {
        LockData localData = new LockData();
        localData.lockPath = ourPath;
        localData.owningThread = Thread.currentThread();
        
        lockData = localData;
    }

    @Override
    protected void clearLockData()
    {
        lockData = null;
    }

    @Override
    protected int lockCount()
    {
        return (lockData != null) ? 1 : 0;
    }
}
