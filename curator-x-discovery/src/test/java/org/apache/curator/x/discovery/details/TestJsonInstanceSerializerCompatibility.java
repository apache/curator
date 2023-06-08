/*
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.Date;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.TestJsonInstanceSerializer;
import org.apache.curator.x.discovery.UriSpec;
import org.junit.jupiter.api.Test;

public class TestJsonInstanceSerializerCompatibility {
    @Test
    public void testCompatibilityMode() throws Exception {
        JsonInstanceSerializer<TestJsonInstanceSerializer.Payload> serializer = new JsonInstanceSerializer<
                TestJsonInstanceSerializer.Payload>(TestJsonInstanceSerializer.Payload.class, true, true);
        ServiceInstance<TestJsonInstanceSerializer.Payload> instance =
                new ServiceInstance<TestJsonInstanceSerializer.Payload>(
                        "name",
                        "id",
                        "address",
                        10,
                        20,
                        new TestJsonInstanceSerializer.Payload("test"),
                        0,
                        ServiceType.DYNAMIC,
                        new UriSpec("{a}/b/{c}"),
                        true);
        byte[] bytes = serializer.serialize(instance);

        OldServiceInstance<TestJsonInstanceSerializer.Payload> oldInstance =
                new OldServiceInstance<TestJsonInstanceSerializer.Payload>(
                        "name",
                        "id",
                        "address",
                        10,
                        20,
                        new TestJsonInstanceSerializer.Payload("test"),
                        0,
                        ServiceType.DYNAMIC,
                        new UriSpec("{a}/b/{c}"));
        ObjectMapper mapper = new ObjectMapper();
        byte[] oldBytes = mapper.writeValueAsBytes(oldInstance);
        assertArrayEquals(bytes, oldBytes, String.format("%s vs %s", new String(bytes), new String(oldBytes)));
    }

    @Test
    public void testBackwardCompatibility() throws Exception {
        JsonInstanceSerializer<TestJsonInstanceSerializer.Payload> serializer = new JsonInstanceSerializer<
                TestJsonInstanceSerializer.Payload>(TestJsonInstanceSerializer.Payload.class, true, true);
        ServiceInstance<TestJsonInstanceSerializer.Payload> instance =
                new ServiceInstance<TestJsonInstanceSerializer.Payload>(
                        "name",
                        "id",
                        "address",
                        10,
                        20,
                        new TestJsonInstanceSerializer.Payload("test"),
                        0,
                        ServiceType.DYNAMIC,
                        new UriSpec("{a}/b/{c}"),
                        false);
        byte[] bytes = serializer.serialize(instance);

        instance = serializer.deserialize(bytes);
        assertTrue(instance.isEnabled()); // passed false for enabled in the ctor but that is lost with
        // compatibleSerializationMode

        ObjectMapper mapper = new ObjectMapper();
        JavaType type = mapper.getTypeFactory().constructType(OldServiceInstance.class);
        OldServiceInstance rawServiceInstance = mapper.readValue(bytes, type);
        TestJsonInstanceSerializer.Payload.class.cast(
                rawServiceInstance.getPayload()); // just to verify that it's the correct type
        //noinspection unchecked
        OldServiceInstance<TestJsonInstanceSerializer.Payload> check =
                (OldServiceInstance<TestJsonInstanceSerializer.Payload>) rawServiceInstance;
        assertEquals(check.getName(), instance.getName());
        assertEquals(check.getId(), instance.getId());
        assertEquals(check.getAddress(), instance.getAddress());
        assertEquals(check.getPort(), instance.getPort());
        assertEquals(check.getSslPort(), instance.getSslPort());
        assertEquals(check.getPayload(), instance.getPayload());
        assertEquals(check.getRegistrationTimeUTC(), instance.getRegistrationTimeUTC());
        assertEquals(check.getServiceType(), instance.getServiceType());
        assertEquals(check.getUriSpec(), instance.getUriSpec());
    }

    @Test
    public void testForwardCompatibility() throws Exception {
        OldServiceInstance<TestJsonInstanceSerializer.Payload> oldInstance =
                new OldServiceInstance<TestJsonInstanceSerializer.Payload>(
                        "name",
                        "id",
                        "address",
                        10,
                        20,
                        new TestJsonInstanceSerializer.Payload("test"),
                        0,
                        ServiceType.DYNAMIC,
                        new UriSpec("{a}/b/{c}"));
        ObjectMapper mapper = new ObjectMapper();
        byte[] oldJson = mapper.writeValueAsBytes(oldInstance);

        JsonInstanceSerializer<TestJsonInstanceSerializer.Payload> serializer = new JsonInstanceSerializer<
                TestJsonInstanceSerializer.Payload>(TestJsonInstanceSerializer.Payload.class);
        ServiceInstance<TestJsonInstanceSerializer.Payload> instance = serializer.deserialize(oldJson);
        assertEquals(oldInstance.getName(), instance.getName());
        assertEquals(oldInstance.getId(), instance.getId());
        assertEquals(oldInstance.getAddress(), instance.getAddress());
        assertEquals(oldInstance.getPort(), instance.getPort());
        assertEquals(oldInstance.getSslPort(), instance.getSslPort());
        assertEquals(oldInstance.getPayload(), instance.getPayload());
        assertEquals(oldInstance.getRegistrationTimeUTC(), instance.getRegistrationTimeUTC());
        assertEquals(oldInstance.getServiceType(), instance.getServiceType());
        assertEquals(oldInstance.getUriSpec(), instance.getUriSpec());
        assertTrue(instance.isEnabled());
    }

    @Test
    public void testFutureChanges() throws Exception {
        TestNewServiceInstance<String> newInstance = new TestNewServiceInstance<String>(
                "name",
                "id",
                "address",
                10,
                20,
                "hey",
                0,
                ServiceType.DYNAMIC,
                new UriSpec("{a}/b/{c}"),
                false,
                "what",
                10101L,
                new Date(),
                new URI("http://hey"));
        byte[] newInstanceBytes = new ObjectMapper().writeValueAsBytes(newInstance);
        JsonInstanceSerializer<String> serializer = new JsonInstanceSerializer<String>(String.class);
        ServiceInstance<String> instance = serializer.deserialize(newInstanceBytes);
        assertEquals(instance.getName(), "name");
        assertEquals(instance.getPayload(), "hey");
        assertEquals(instance.isEnabled(), false);
    }
}
