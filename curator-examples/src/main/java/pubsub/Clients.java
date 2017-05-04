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

import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModelSpecBuilder;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework2;
import org.apache.zookeeper.CreateMode;
import pubsub.messages.LocationAvailable;
import pubsub.messages.UserCreated;
import pubsub.models.Group;
import pubsub.models.Instance;
import pubsub.models.InstanceType;
import pubsub.models.Priority;
import java.util.concurrent.TimeUnit;

public class Clients
{
    /**
     * A client template for LocationAvailable instances
     */
    public static final TypedModeledFramework2<LocationAvailable, Group, Priority> locationAvailableClient = TypedModeledFramework2.from(
        ModeledFramework.builder(),
        builder(LocationAvailable.class),
        "/root/pubsub/messages/locations/{id}/{id}"
    );

    /**
     * A client template for UserCreated instances
     */
    public static final TypedModeledFramework2<UserCreated, Group, Priority> userCreatedClient = TypedModeledFramework2.from(
        ModeledFramework.builder(),
        builder(UserCreated.class),
        "/root/pubsub//messages/users/{id}/{id}"
    );

    /**
     * A client template for Instance instances
     */
    public static final TypedModeledFramework<Instance, InstanceType> instanceClient = TypedModeledFramework.from(
        ModeledFramework.builder(),
        builder(Instance.class),
        "/root/pubsub//instances/{id}"
    );

    private static <T> ModelSpecBuilder<T> builder(Class<T> clazz)
    {
        return ModelSpec.builder(JacksonModelSerializer.build(clazz))
            .withTtl(TimeUnit.MINUTES.toMillis(10)) // for our pub-sub example, messages are valid for 10 minutes
            .withCreateMode(CreateMode.PERSISTENT_WITH_TTL)
            ;
    }

    private Clients()
    {
    }
}
