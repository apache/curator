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

import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pubsub.messages.LocationAvailable;
import pubsub.messages.UserCreated;
import pubsub.models.Group;
import pubsub.models.Instance;
import pubsub.models.Message;
import pubsub.models.Priority;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Publisher
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AsyncCuratorFramework client;

    public Publisher(AsyncCuratorFramework client)
    {
        this.client = Objects.requireNonNull(client, "client cannot be null");
    }

    /**
     * Publish the given instance using the Instance client template
     *
     * @param instance instance to publish
     */
    public void publishInstance(Instance instance)
    {
        ModeledFramework<Instance> resolvedClient = Clients.instanceClient.resolved(client, instance.getType());
        resolvedClient.set(instance).exceptionally(e -> {
            log.error("Could not publish instance: " + instance, e);
            return null;
        });
    }

    /**
     * Publish the given instances using the Instance client template in a transaction
     *
     * @param instances instances to publish
     */
    public void publishInstances(List<Instance> instances)
    {
        List<CuratorOp> operations = instances.stream()
            .map(instance -> Clients.instanceClient
                .resolved(client, instance.getType())
                .createOp(instance)
            )
            .collect(Collectors.toList());
        client.transaction().forOperations(operations).exceptionally(e -> {
            log.error("Could not publish instances: " + instances, e);
            return null;
        });
    }

    /**
     * Publish the given LocationAvailable using the LocationAvailable client template
     *
     * @param group group
     * @param locationAvailable message to publish
     */
    public void publishLocationAvailable(Group group, LocationAvailable locationAvailable)
    {
        publishMessage(Clients.locationAvailableClient, group, locationAvailable);
    }

    /**
     * Publish the given UserCreated using the UserCreated client template
     *
     * @param group group
     * @param userCreated message to publish
     */
    public void publishUserCreated(Group group, UserCreated userCreated)
    {
        publishMessage(Clients.userCreatedClient, group, userCreated);
    }

    /**
     * Publish the given LocationAvailables using the LocationAvailable client template in a transaction
     *
     * @param group group
     * @param locationsAvailable messages to publish
     */
    public void publishLocationsAvailable(Group group, List<LocationAvailable> locationsAvailable)
    {
        publishMessages(Clients.locationAvailableClient, group, locationsAvailable);
    }

    /**
     * Publish the given UserCreateds using the UserCreated client template in a transaction
     *
     * @param group group
     * @param usersCreated messages to publish
     */
    public void publishUsersCreated(Group group, List<UserCreated> usersCreated)
    {
        publishMessages(Clients.userCreatedClient, group, usersCreated);
    }

    private <T extends Message> void publishMessage(TypedModeledFramework2<T, Group, Priority> typedClient, Group group, T message)
    {
        ModeledFramework<T> resolvedClient = typedClient.resolved(client, group, message.getPriority());
        resolvedClient.set(message).exceptionally(e -> {
            log.error("Could not publish message: " + message, e);
            return null;
        });
    }

    private <T extends Message> void publishMessages(TypedModeledFramework2<T, Group, Priority> typedClient, Group group, List<T> messages)
    {
        List<CuratorOp> operations = messages.stream()
            .map(message -> typedClient
                    .resolved(client, group, message.getPriority())
                    .createOp(message)
                )
            .collect(Collectors.toList());
        client.transaction().forOperations(operations).exceptionally(e -> {
            log.error("Could not publish messages: " + messages, e);
            return null;
        });
    }
}
