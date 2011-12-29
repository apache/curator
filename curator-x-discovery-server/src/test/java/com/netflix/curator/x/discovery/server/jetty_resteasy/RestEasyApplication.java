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

package com.netflix.curator.x.discovery.server.jetty_resteasy;

import com.google.common.collect.Sets;
import javax.ws.rs.core.Application;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class RestEasyApplication extends Application
{
    public static final AtomicReference<RestEasySingletons>     singletonsRef = new AtomicReference<RestEasySingletons>(new RestEasySingletons());

    @Override
    public Set<Class<?>> getClasses()
    {
        Set<Class<?>>       classes = Sets.newHashSet();
        classes.add(StringDiscoveryResource.class);
        return classes;
    }

    @Override
    public Set<Object> getSingletons()
    {
        Set<Object>     singletons = Sets.newHashSet();
        singletons.add(singletonsRef.get().contextSingleton);
        singletons.add(singletonsRef.get().serviceNamesMarshallerSingleton);
        singletons.add(singletonsRef.get().serviceInstanceMarshallerSingleton);
        singletons.add(singletonsRef.get().serviceInstancesMarshallerSingleton);
        return singletons;
    }
}
