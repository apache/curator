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
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestUriSpec {
    @Test
    public void testScheme() {
        UriSpec spec = new UriSpec("{scheme}://foo.com");

        ServiceInstanceBuilder<Void> builder = new ServiceInstanceBuilder<Void>();
        builder.id("x");
        builder.name("foo");
        builder.port(5);
        ServiceInstance<Void> instance = builder.build();
        assertEquals(spec.build(instance), "http://foo.com");

        builder.sslPort(5);
        instance = builder.build();
        assertEquals(spec.build(instance), "https://foo.com");
    }

    @Test
    public void testFromInstance() {
        ServiceInstanceBuilder<Void> builder = new ServiceInstanceBuilder<Void>();
        builder.address("1.2.3.4");
        builder.name("foo");
        builder.id("bar");
        builder.port(5);
        builder.sslPort(6);
        builder.registrationTimeUTC(789);
        builder.serviceType(ServiceType.PERMANENT);
        ServiceInstance<Void> instance = builder.build();

        UriSpec spec = new UriSpec(
                "{scheme}://{address}:{port}:{ssl-port}/{name}/{id}/{registration-time-utc}/{service-type}");

        Map<String, Object> m = Maps.newHashMap();
        m.put("scheme", "test");
        assertEquals(spec.build(instance, m), "test://1.2.3.4:5:6/foo/bar/789/permanent");
    }

    @Test
    public void testEscapes() {
        UriSpec spec = new UriSpec("{one}two-three-{[}four{]}-five{six}");
        Iterator<UriSpec.Part> iterator = spec.iterator();
        checkPart(iterator.next(), "one", true);
        checkPart(iterator.next(), "two-three-", false);
        checkPart(iterator.next(), "[", true);
        checkPart(iterator.next(), "four", false);
        checkPart(iterator.next(), "]", true);
        checkPart(iterator.next(), "-five", false);
        checkPart(iterator.next(), "six", true);

        Map<String, Object> m = Maps.newHashMap();
        m.put("one", 1);
        m.put("six", 6);
        assertEquals(spec.build(m), "1two-three-{four}-five6");
    }

    @Test
    public void testBasic() {
        UriSpec spec = new UriSpec("{one}{two}three-four-five{six}seven{eight}");
        Iterator<UriSpec.Part> iterator = spec.iterator();
        checkPart(iterator.next(), "one", true);
        checkPart(iterator.next(), "two", true);
        checkPart(iterator.next(), "three-four-five", false);
        checkPart(iterator.next(), "six", true);
        checkPart(iterator.next(), "seven", false);
        checkPart(iterator.next(), "eight", true);
    }

    private void checkPart(UriSpec.Part p, String value, boolean isVariable) {
        assertEquals(p.getValue(), value);
        assertEquals(p.isVariable(), isVariable);
    }
}
