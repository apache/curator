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
package pubsub.models;

import org.apache.curator.x.async.modeled.NodeName;
import java.util.Objects;
import java.util.UUID;

public abstract class Message implements NodeName
{
    private final String id;
    private final Priority priority;

    protected Message()
    {
        this(UUID.randomUUID().toString(), Priority.low);
    }

    protected Message(Priority priority)
    {
        this(UUID.randomUUID().toString(), priority);
    }

    protected Message(String id, Priority priority)
    {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.priority = Objects.requireNonNull(priority, "messageType cannot be null");
    }

    public String getId()
    {
        return id;
    }

    public Priority getPriority()
    {
        return priority;
    }

    @Override
    public String nodeName()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return "Message{" + "id='" + id + '\'' + ", priority=" + priority + '}';
    }
}
