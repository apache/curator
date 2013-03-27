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
package org.apache.curator.x.discovery.strategies;

import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceProvider;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This strategy uses a master strategy to pick the initial instance. Once picked,
 * that instance is always returned. If, however, the currently selected instance
 * is no longer in the list, the master strategy is used to pick a new instance.
 */
public class StickyStrategy<T> implements ProviderStrategy<T>
{
    private final ProviderStrategy<T>                   masterStrategy;
    private final AtomicReference<ServiceInstance<T>>   ourInstance = new AtomicReference<ServiceInstance<T>>(null);
    private final AtomicInteger                         instanceNumber = new AtomicInteger(-1);

    /**
     * @param masterStrategy the strategy to use for picking the sticky instance
     */
    public StickyStrategy(ProviderStrategy<T> masterStrategy)
    {
        this.masterStrategy = masterStrategy;
    }

    @Override
    public ServiceInstance<T> getInstance(InstanceProvider<T> instanceProvider) throws Exception
    {
        final List<ServiceInstance<T>>    instances = instanceProvider.getInstances();

        {
            ServiceInstance<T>                localOurInstance = ourInstance.get();
            if ( !instances.contains(localOurInstance) )
            {
                ourInstance.compareAndSet(localOurInstance, null);
            }
        }
        
        if ( ourInstance.get() == null )
        {
            ServiceInstance<T> instance = masterStrategy.getInstance
            (
                new InstanceProvider<T>()
                {
                    @Override
                    public List<ServiceInstance<T>> getInstances() throws Exception
                    {
                       return instances;
                    }
                }
            );
            if ( ourInstance.compareAndSet(null, instance) )
            {
                instanceNumber.incrementAndGet();
            }
        }
        return ourInstance.get();
    }

    /**
     * Each time a new instance is picked, an internal counter is incremented. This way you
     * can track when/if the instance changes. The instance can change when the selected instance
     * is not in the current list of instances returned by the instance provider
     *
     * @return instance number
     */
    public int getInstanceNumber()
    {
        return instanceNumber.get();
    }
}
