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

import org.apache.curator.x.async.modeled.typed.TypedZPath;
import org.apache.curator.x.async.modeled.typed.TypedZPath2;
import pubsub.models.Group;
import pubsub.models.InstanceType;
import pubsub.models.Priority;

public class Paths
{
    private static final String basePath = "/root/pubsub";

    /**
     * Represents a path for LocationAvailable models that is parameterized with a Group and a Priority
     */
    public static final TypedZPath2<Group, Priority> locationAvailablePath = TypedZPath2.from(basePath + "/messages/locations/{id}/{id}");

    /**
     * Represents a path for UserCreated models that is parameterized with a Group and a Priority
     */
    public static final TypedZPath2<Group, Priority> userCreatedPath = TypedZPath2.from(basePath + "/messages/users/{id}/{id}");

    /**
     * Represents a path for Instance models that is parameterized with a InstanceType
     */
    public static final TypedZPath<InstanceType> instancesPath = TypedZPath.from(basePath + "/instances/{id}");

    private Paths()
    {
    }
}
