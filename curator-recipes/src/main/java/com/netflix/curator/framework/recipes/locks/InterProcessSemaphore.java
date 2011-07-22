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
 * A re-entrant mutex that works across JVMs. Uses Zookeeper to hold the lock. All processes in all JVMs that
 * use the same lock path will achieve an inter-process critical section. Further, this mutex is
 * "fair" - each user will get the mutex in the order requested (from ZK's point of view)
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

    public InterProcessSemaphore(CuratorFramework client, String path, int numberOfLeases)
    {
        this(client, path, numberOfLeases, null);
    }

    public InterProcessSemaphore(CuratorFramework client, String path, int numberOfLeases, ClientClosingListener<InterProcessSemaphore> clientClosingListener)
    {
        super(client, path, LOCK_NAME, clientClosingListener, numberOfLeases);
    }

    public void acquire() throws Exception
    {
        internalAcquire();
    }

    public boolean acquire(long time, TimeUnit unit) throws Exception
    {
        String      lockPath = internalAcquire(time, unit);
        return (lockPath != null);
    }

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
