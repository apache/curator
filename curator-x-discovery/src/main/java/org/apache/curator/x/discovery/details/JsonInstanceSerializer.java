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
package org.apache.curator.x.discovery.details;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.x.discovery.ServiceInstance;
import java.io.ByteArrayOutputStream;

/**
 * A serializer that uses Jackson to serialize/deserialize as JSON. IMPORTANT: The instance
 * payload must support Jackson
 */
public class JsonInstanceSerializer<T> implements InstanceSerializer<T>
{
    private final ObjectMapper      mapper;
    private final Class<T>          payloadClass;
    private final JavaType          type;

    /**
     * @param payloadClass used to validate payloads when deserializing
     */
    public JsonInstanceSerializer(Class<T> payloadClass)
    {
        this.payloadClass = payloadClass;
        mapper = new ObjectMapper();
        type = mapper.getTypeFactory().constructType(ServiceInstance.class);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public ServiceInstance<T> deserialize(byte[] bytes) throws Exception
    {
        ServiceInstance rawServiceInstance = mapper.readValue(bytes, type);
        payloadClass.cast(rawServiceInstance.getPayload()); // just to verify that it's the correct type
        return (ServiceInstance<T>)rawServiceInstance;
    }

    @Override
    public byte[] serialize(ServiceInstance<T> instance) throws Exception
    {
        ByteArrayOutputStream           out = new ByteArrayOutputStream();
        mapper.writeValue(out, instance);
        return out.toByteArray();
    }
}
