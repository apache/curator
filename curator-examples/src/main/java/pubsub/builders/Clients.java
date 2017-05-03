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

import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework2;
import pubsub.messages.LocationAvailable;
import pubsub.messages.UserCreated;
import pubsub.models.Group;
import pubsub.models.Instance;
import pubsub.models.InstanceType;
import pubsub.models.Priority;

public class Clients
{
    /**
     * A client template for LocationAvailable instances
     */
    public static final TypedModeledFramework2<LocationAvailable, Group, Priority> locationAvailableClient = TypedModeledFramework2.from(
        ModeledFramework.builder(),             // our client will use only defaults
        ModelSpecs.locationAvailableModelSpec   // the LocationAvailable model spec
    );

    /**
     * A client template for UserCreated instances
     */
    public static final TypedModeledFramework2<UserCreated, Group, Priority> userCreatedClient = TypedModeledFramework2.from(
        ModeledFramework.builder(),             // our client will use only defaults
        ModelSpecs.userCreatedModelSpec         // the UserCreated model spec
    );

    /**
     * A client template for Instance instances
     */
    public static final TypedModeledFramework<Instance, InstanceType> instanceClient = TypedModeledFramework.from(
        ModeledFramework.builder(),             // our client will use only defaults
        ModelSpecs.instanceModelSpec            // the Instance model spec
    );

    private Clients()
    {
    }
}
