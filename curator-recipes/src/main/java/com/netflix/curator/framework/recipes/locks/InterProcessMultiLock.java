/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.curator.framework.recipes.locks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.curator.framework.CuratorFramework;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.reverse;

public class InterProcessMultiLock implements InterProcessLock
{
    private final List<InterProcessLock> locks;

    /**
     * Creates a multi lock of {@link InterProcessMutex}s
     *
     * @param client the client
     * @param paths list of paths to manage in the order that they are to be locked
     */
    public InterProcessMultiLock(CuratorFramework client, List<String> paths)
    {
        this(makeLocks(client, paths));
    }

    /**
     * Creates a multi lock of any type of inter process lock
     *
     * @param locks the locks
     */
    public InterProcessMultiLock(List<InterProcessLock> locks)
    {
        this.locks = ImmutableList.copyOf(locks);
    }

    private static List<InterProcessLock> makeLocks(CuratorFramework client, List<String> paths)
    {
        ImmutableList.Builder<InterProcessLock> builder = ImmutableList.builder();
        for ( String path : paths )
        {
            InterProcessLock        lock = new InterProcessMutex(client, path);
            builder.add(lock);
        }
        return builder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void acquire() throws Exception
    {
        acquire(-1, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acquire(long time, TimeUnit unit) throws Exception
    {
        Exception                   exception = null;
        List<InterProcessLock>      acquired = Lists.newArrayList();
        boolean                     success = true;
        for ( InterProcessLock lock : locks )
        {
            try
            {
                if ( unit == null )
                {
                    lock.acquire();
                    acquired.add(lock);
                }
                else
                {
                    if ( lock.acquire(time, unit) )
                    {
                        acquired.add(lock);
                    }
                    else
                    {
                        success = false;
                        break;
                    }
                }
            }
            catch ( Exception e )
            {
                success = false;
                exception = e;
            }
        }

        if ( !success )
        {
            for ( InterProcessLock lock : reverse(acquired) )
            {
                try
                {
                    lock.release();
                }
                catch ( Exception e )
                {
                    // ignore
                }
            }
        }

        if ( exception != null )
        {
            throw exception;
        }
        
        return success;
    }

    /**
     * {@inheritDoc}
     *
     * <p>NOTE: locks are released in the reverse order that they were acquired.</p>
     */
    @Override
    public synchronized void release() throws Exception
    {
        Exception       baseException = null;

        for ( InterProcessLock lock : reverse(locks) )
        {
            try
            {
                lock.release();
            }
            catch ( Exception e )
            {
                if ( baseException == null )
                {
                    baseException = e;
                }
                else
                {
                    baseException = new Exception(baseException);
                }
            }
        }

        if ( baseException != null )
        {
            throw baseException;
        }
    }

    @Override
    public synchronized boolean isAcquiredInThisProcess()
    {
        // it's subjective what the correct meaning is here - I choose to return true
        // only if all of the locks are acquired

        for ( InterProcessLock lock : locks )
        {
            if ( !lock.isAcquiredInThisProcess() )
            {
                return false;
            }
        }
        return true;
    }
}
