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
package pubsub.builders;

import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModelSpecBuilder;
import org.apache.curator.x.async.modeled.typed.TypedModelSpec;
import org.apache.curator.x.async.modeled.typed.TypedModelSpec2;
import org.apache.zookeeper.CreateMode;
import pubsub.messages.LocationAvailable;
import pubsub.messages.UserCreated;
import pubsub.models.Group;
import pubsub.models.Instance;
import pubsub.models.InstanceType;
import pubsub.models.Priority;
import java.util.concurrent.TimeUnit;

public class ModelSpecs
{
    /**
     * A model spec for LocationAvailable instances
     */
    public static final TypedModelSpec2<LocationAvailable, Group, Priority> locationAvailableModelSpec = TypedModelSpec2.from(
        builder(LocationAvailable.class),   // creates a standard JSON serializer
        Paths.locationAvailablePath         // use the path for LocationAvailable instances
    );

    /**
     * A model spec for UserCreated instances
     */
    public static final TypedModelSpec2<UserCreated, Group, Priority> userCreatedModelSpec = TypedModelSpec2.from(
        builder(UserCreated.class),         // creates a standard JSON serializer
        Paths.userCreatedPath               // use the path for UserCreated instances
    );

    /**
     * A model spec for Instance instances
     */
    public static final TypedModelSpec<Instance, InstanceType> instanceModelSpec = TypedModelSpec.from(
        builder(Instance.class),            // creates a standard JSON serializer
        Paths.instancesPath                 // use the path for Instance instances
    );

    private static <T> ModelSpecBuilder<T> builder(Class<T> clazz)
    {
        return ModelSpec.builder(JacksonModelSerializer.build(clazz))
            .withTtl(TimeUnit.MINUTES.toMillis(10)) // for our pub-sub example, messages are valid for 10 minutes
            .withCreateMode(CreateMode.PERSISTENT_WITH_TTL)
            ;
    }

    private ModelSpecs()
    {
    }
}
