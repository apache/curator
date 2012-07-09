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

package com.netflix.curator.x.discovery.server.rest;

import com.google.common.base.Preconditions;
import com.netflix.curator.utils.ThreadUtils;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 * A background task that purges stale registrations. You should allocate a singleton
 * of this class, call {@link #start()} and then call {@link #close()} when your application
 * is shutting down.
 */
public class InstanceCleanup implements Closeable
{
    private static final Logger         log = LoggerFactory.getLogger(InstanceCleanup.class);

    private final ServiceDiscovery<Object>  discovery;
    private final int                       instanceRefreshMs;
    private final ExecutorService           service = ThreadUtils.newSingleThreadExecutor("InstanceCleanup");

    /**
     * @param discovery the service being monitored
     * @param instanceRefreshMs time in milliseconds to consider a registration stale
     */
    public InstanceCleanup(ServiceDiscovery<?> discovery, int instanceRefreshMs)
    {
        //noinspection unchecked
        this.discovery = (ServiceDiscovery<Object>)discovery;   // this cast is safe - this class never accesses the payload
        this.instanceRefreshMs = instanceRefreshMs;
    }

    /**
     * Start the task
     */
    public void     start()
    {
        Preconditions.checkArgument(!service.isShutdown(), "already started");

        service.submit
        (
            new Runnable()
            {
                @Override
                public void run()
                {
                    doWork();
                }
            }
        );
    }

    @Override
    public void close() throws IOException
    {
        Preconditions.checkArgument(!service.isShutdown(), "not started");
        service.shutdownNow();
    }

    private void doWork()
    {
        while ( !Thread.currentThread().isInterrupted() )
        {
            try
            {
                Thread.sleep(instanceRefreshMs);
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                break;
            }

            try
            {
                for ( String name : discovery.queryForNames() )
                {
                    checkService(name);
                }
            }
            catch ( Exception e )
            {
                log.error("GC for service names", e);
            }
        }
    }

    private void checkService(String name)
    {
        try
        {
            Collection<ServiceInstance<Object>>     instances = discovery.queryForInstances(name);
            for ( ServiceInstance<Object> instance : instances )
            {
                if ( instance.getServiceType() != ServiceType.PERMANENT )
                {
                    if ( (System.currentTimeMillis() - instance.getRegistrationTimeUTC()) > instanceRefreshMs )
                    {
                        discovery.unregisterService(instance);
                    }
                }
            }
        }
        catch ( Exception e )
        {
            log.error(String.format("GC for service: %s", name), e);
        }
    }
}
