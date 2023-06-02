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

package org.apache.curator.x.discovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.junit.jupiter.api.Test;

public class TestJsonInstanceSerializer {
    @Test
    public void testBasic() throws Exception {
        JsonInstanceSerializer<String> serializer = new JsonInstanceSerializer<String>(String.class);
        ServiceInstance<String> instance = new ServiceInstance<String>(
                "name", "id", "address", 10, 20, "payload", 0, ServiceType.DYNAMIC, new UriSpec("{a}/b/{c}"), true);
        byte[] bytes = serializer.serialize(instance);

        ServiceInstance<String> rhs = serializer.deserialize(bytes);
        assertEquals(instance, rhs);
        assertEquals(instance.getId(), rhs.getId());
        assertEquals(instance.getName(), rhs.getName());
        assertEquals(instance.getPayload(), rhs.getPayload());
        assertEquals(instance.getAddress(), rhs.getAddress());
        assertEquals(instance.getPort(), rhs.getPort());
        assertEquals(instance.getSslPort(), rhs.getSslPort());
        assertEquals(instance.getUriSpec(), rhs.getUriSpec());
        assertEquals(instance.isEnabled(), rhs.isEnabled());
    }

    @Test
    public void testWrongPayloadType() throws Exception {
        JsonInstanceSerializer<String> stringSerializer = new JsonInstanceSerializer<String>(String.class);
        JsonInstanceSerializer<Double> doubleSerializer = new JsonInstanceSerializer<Double>(Double.class);

        byte[] bytes = stringSerializer.serialize(new ServiceInstance<String>(
                "name", "id", "address", 10, 20, "payload", 0, ServiceType.DYNAMIC, new UriSpec("{a}/b/{c}"), true));
        try {
            doubleSerializer.deserialize(bytes);
            fail();
        } catch (ClassCastException e) {
            // correct
        }
    }

    @Test
    public void testNoPayload() throws Exception {
        JsonInstanceSerializer<Void> serializer = new JsonInstanceSerializer<Void>(Void.class);
        ServiceInstance<Void> instance = new ServiceInstance<Void>(
                "name", "id", "address", 10, 20, null, 0, ServiceType.DYNAMIC, new UriSpec("{a}/b/{c}"), true);
        byte[] bytes = serializer.serialize(instance);

        ServiceInstance<Void> rhs = serializer.deserialize(bytes);
        assertEquals(instance, rhs);
        assertEquals(instance.getId(), rhs.getId());
        assertEquals(instance.getName(), rhs.getName());
        assertEquals(instance.getPayload(), rhs.getPayload());
        assertEquals(instance.getAddress(), rhs.getAddress());
        assertEquals(instance.getPort(), rhs.getPort());
        assertEquals(instance.getSslPort(), rhs.getSslPort());
        assertEquals(instance.getUriSpec(), rhs.getUriSpec());
        assertEquals(instance.isEnabled(), rhs.isEnabled());
    }

    @Test
    public void testNoEnabledState() throws Exception {
        JsonInstanceSerializer<Void> serializer = new JsonInstanceSerializer<Void>(Void.class);
        byte[] bytes = "{}".getBytes("utf-8");

        ServiceInstance<Void> instance = serializer.deserialize(bytes);
        assertTrue(instance.isEnabled(), "Instance that has no 'enabled' should be assumed enabled");
    }

    @Test
    public void testPayloadAsList() throws Exception {
        JsonInstanceSerializer<Object> serializer = new JsonInstanceSerializer<Object>(Object.class, false);
        List<String> payload = new ArrayList<String>();
        payload.add("Test value 1");
        payload.add("Test value 2");
        ServiceInstance<Object> instance = new ServiceInstance<Object>(
                "name", "id", "address", 10, 20, payload, 0, ServiceType.DYNAMIC, new UriSpec("{a}/b/{c}"), false);
        byte[] bytes = serializer.serialize(instance);

        ServiceInstance<Object> rhs = serializer.deserialize(bytes);
        assertEquals(instance, rhs);
        assertEquals(instance.getId(), rhs.getId());
        assertEquals(instance.getName(), rhs.getName());
        assertEquals(instance.getPayload(), rhs.getPayload());
        assertEquals(instance.getAddress(), rhs.getAddress());
        assertEquals(instance.getPort(), rhs.getPort());
        assertEquals(instance.getSslPort(), rhs.getSslPort());
        assertEquals(instance.getUriSpec(), rhs.getUriSpec());
        assertEquals(instance.isEnabled(), rhs.isEnabled());
    }

    @Test
    public void testPayloadAsMap() throws Exception {
        JsonInstanceSerializer<Object> serializer = new JsonInstanceSerializer<Object>(Object.class, false);
        Map<String, String> payload = new HashMap<String, String>();
        payload.put("1", "Test value 1");
        payload.put("2", "Test value 2");
        ServiceInstance<Object> instance = new ServiceInstance<Object>(
                "name", "id", "address", 10, 20, payload, 0, ServiceType.DYNAMIC, new UriSpec("{a}/b/{c}"), false);
        byte[] bytes = serializer.serialize(instance);

        ServiceInstance<Object> rhs = serializer.deserialize(bytes);
        assertEquals(instance, rhs);
        assertEquals(instance.getId(), rhs.getId());
        assertEquals(instance.getName(), rhs.getName());
        assertEquals(instance.getPayload(), rhs.getPayload());
        assertEquals(instance.getAddress(), rhs.getAddress());
        assertEquals(instance.getPort(), rhs.getPort());
        assertEquals(instance.getSslPort(), rhs.getSslPort());
        assertEquals(instance.getUriSpec(), rhs.getUriSpec());
        assertEquals(instance.isEnabled(), rhs.isEnabled());
    }

    @Test
    public void testPayloadClass() throws Exception {
        JsonInstanceSerializer<Payload> serializer = new JsonInstanceSerializer<Payload>(Payload.class);
        Payload payload = new Payload();
        payload.setVal("Test value");
        ServiceInstance<Payload> instance = new ServiceInstance<Payload>(
                "name", "id", "address", 10, 20, payload, 0, ServiceType.DYNAMIC, new UriSpec("{a}/b/{c}"), true);
        byte[] bytes = serializer.serialize(instance);

        ServiceInstance<Payload> rhs = serializer.deserialize(bytes);
        assertEquals(instance, rhs);
        assertEquals(instance.getId(), rhs.getId());
        assertEquals(instance.getName(), rhs.getName());
        assertEquals(instance.getPayload(), rhs.getPayload());
        assertEquals(instance.getAddress(), rhs.getAddress());
        assertEquals(instance.getPort(), rhs.getPort());
        assertEquals(instance.getSslPort(), rhs.getSslPort());
        assertEquals(instance.getUriSpec(), rhs.getUriSpec());
        assertEquals(instance.isEnabled(), rhs.isEnabled());
    }

    public static class Payload {
        private String val;

        public String getVal() {
            return val;
        }

        public Payload() {}

        public Payload(String val) {
            this.val = val;
        }

        public void setVal(String val) {
            this.val = val;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null || !(other instanceof Payload)) return false;
            String otherVal = ((Payload) other).getVal();
            if (val == null) return val == otherVal;
            return val.equals(otherVal);
        }
    }
}
