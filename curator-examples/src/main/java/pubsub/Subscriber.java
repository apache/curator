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
package pubsub;

import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework2;
import pubsub.messages.LocationAvailable;
import pubsub.messages.UserCreated;
import pubsub.models.Group;
import pubsub.models.Instance;
import pubsub.models.InstanceType;
import pubsub.models.Message;
import pubsub.models.Priority;

public class Subscriber
{
    private final AsyncCuratorFramework client;

    public Subscriber(AsyncCuratorFramework client)
    {
        this.client = client;
    }

    /**
     * Start a subscriber (a CachedModeledFramework instance) using the LocationAvailable client template
     *
     * @param group group to listen for
     * @param priority priority to listen for
     * @return CachedModeledFramework instance (already started)
     */
    public CachedModeledFramework<LocationAvailable> startLocationAvailableSubscriber(Group group, Priority priority)
    {
        return startSubscriber(Clients.locationAvailableClient, group, priority);
    }

    /**
     * Start a subscriber (a CachedModeledFramework instance) using the UserCreated client template
     *
     * @param group group to listen for
     * @param priority priority to listen for
     * @return CachedModeledFramework instance (already started)
     */
    public CachedModeledFramework<UserCreated> startUserCreatedSubscriber(Group group, Priority priority)
    {
        return startSubscriber(Clients.userCreatedClient, group, priority);
    }

    /**
     * Start a subscriber (a CachedModeledFramework instance) using the Instance client template
     *
     * @param instanceType type to listen for
     * @return CachedModeledFramework instance (already started)
     */
    public CachedModeledFramework<Instance> startInstanceSubscriber(InstanceType instanceType)
    {
        CachedModeledFramework<Instance> resolved = Clients.instanceClient.resolved(client, instanceType).cached();
        resolved.start();
        return resolved;
    }

    private <T extends Message> CachedModeledFramework<T> startSubscriber(TypedModeledFramework2<T, Group, Priority> typedClient, Group group, Priority priority)
    {
        CachedModeledFramework<T> resolved = typedClient.resolved(client, group, priority).cached();
        resolved.start();
        return resolved;
    }
}
