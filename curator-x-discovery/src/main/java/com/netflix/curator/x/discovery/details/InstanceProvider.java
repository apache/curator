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

package com.netflix.curator.x.discovery.details;

import com.netflix.curator.x.discovery.ServiceInstance;
import java.util.List;

/**
 * Provides a set of available instances for a service so that a strategy can pick one of them
 */
public interface InstanceProvider<T>
{
    /**
     * Return the current available set of instances
     * @return instances
     * @throws Exception any errors
     */
    public List<ServiceInstance<T>>      getInstances() throws Exception;
}
