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
package pubsub.messages;

import pubsub.models.Message;
import pubsub.models.Priority;
import java.time.Duration;
import java.util.Objects;

public class LocationAvailable extends Message
{
    private final String name;
    private final Duration availableUntil;

    public LocationAvailable()
    {
        this(Priority.low, "", Duration.ZERO);
    }

    public LocationAvailable(Priority priority, String name, Duration availableUntil)
    {
        super(priority);
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.availableUntil = Objects.requireNonNull(availableUntil, "availableUntil cannot be null");
    }

    public LocationAvailable(String id, Priority priority, String name, Duration availableUntil)
    {
        super(id, priority);
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.availableUntil = Objects.requireNonNull(availableUntil, "availableUntil cannot be null");
    }

    @Override
    public String toString()
    {
        return "LocationAvailable{" + "name='" + name + '\'' + ", availableUntil=" + availableUntil + "} " + super.toString();
    }
}
