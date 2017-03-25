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

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.x.discovery.ServiceInstance;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

/**
 * A serializer that uses Jackson to serialize/deserialize as JSON. IMPORTANT: The instance
 * payload must support Jackson
 */
public class JsonInstanceSerializer<T> implements InstanceSerializer<T>
{
    private final ObjectMapper mapper;
    private final Class<T> payloadClass;
    private final boolean compatibleSerializationMode;
    private final JavaType type;

    /**
     * @param payloadClass used to validate payloads when deserializing
     */
    public JsonInstanceSerializer(Class<T> payloadClass)
    {
        this(payloadClass, false, false);
    }

    /**
     * CURATOR-275 introduced a new field into {@link org.apache.curator.x.discovery.ServiceInstance}. This caused a potential
     * {@link org.codehaus.jackson.map.exc.UnrecognizedPropertyException} in older clients that
     * read newly serialized ServiceInstances. If you are susceptible to this you should set the
     * serializer to be an instance of {@link org.apache.curator.x.discovery.details.JsonInstanceSerializer}
     * with <code>compatibleSerializationMode</code> set to true. IMPORTANT: when this is done, the new <code>enabled</code>
     * field of ServiceInstance is <strong>not</strong> serialized.
     *
     * @param payloadClass used to validate payloads when deserializing
     * @param compatibleSerializationMode pass true to serialize in a manner that supports clients pre-CURATOR-275
     */
    public JsonInstanceSerializer(Class<T> payloadClass, boolean compatibleSerializationMode)
    {
        this(payloadClass, compatibleSerializationMode, false);
    }

    @VisibleForTesting
    JsonInstanceSerializer(Class<T> payloadClass, boolean compatibleSerializationMode, boolean failOnUnknownProperties)
    {
        this.payloadClass = payloadClass;
        this.compatibleSerializationMode = compatibleSerializationMode;
        mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, failOnUnknownProperties);
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
        if ( compatibleSerializationMode )
        {
            OldServiceInstance<T> compatible = new OldServiceInstance<T>(instance.getName(), instance.getId(), instance.getAddress(), instance.getPort(), instance.getSslPort(), instance.getPayload(), instance.getRegistrationTimeUTC(), instance.getServiceType(), instance.getUriSpec());
            return mapper.writeValueAsBytes(compatible);
        }
        return mapper.writeValueAsBytes(instance);
    }
}
