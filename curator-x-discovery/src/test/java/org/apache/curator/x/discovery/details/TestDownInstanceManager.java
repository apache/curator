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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.TimeUnit;
import org.apache.curator.x.discovery.DownInstancePolicy;
import org.apache.curator.x.discovery.ServiceInstance;
import org.junit.jupiter.api.Test;

public class TestDownInstanceManager {
    private static final DownInstancePolicy debugDownInstancePolicy = new DownInstancePolicy(1, TimeUnit.SECONDS, 1);
    private static final DownInstancePolicy debugMultiDownInstancePolicy =
            new DownInstancePolicy(1, TimeUnit.SECONDS, 2);

    @Test
    public void testBasic() throws Exception {
        ServiceInstance<Void> instance1 =
                ServiceInstance.<Void>builder().name("hey").id("1").build();
        ServiceInstance<Void> instance2 =
                ServiceInstance.<Void>builder().name("hey").id("2").build();

        DownInstanceManager<Void> downInstanceManager = new DownInstanceManager<Void>(debugDownInstancePolicy);
        assertTrue(downInstanceManager.apply(instance1));
        assertTrue(downInstanceManager.apply(instance2));

        downInstanceManager.add(instance1);
        assertFalse(downInstanceManager.apply(instance1));
        assertTrue(downInstanceManager.apply(instance2));
    }

    @Test
    public void testThreshold() throws Exception {
        ServiceInstance<Void> instance1 =
                ServiceInstance.<Void>builder().name("hey").id("1").build();
        ServiceInstance<Void> instance2 =
                ServiceInstance.<Void>builder().name("hey").id("2").build();

        DownInstanceManager<Void> downInstanceManager = new DownInstanceManager<Void>(debugMultiDownInstancePolicy);
        assertTrue(downInstanceManager.apply(instance1));
        assertTrue(downInstanceManager.apply(instance2));

        downInstanceManager.add(instance1);
        assertTrue(downInstanceManager.apply(instance1));
        assertTrue(downInstanceManager.apply(instance2));

        downInstanceManager.add(instance1);
        assertFalse(downInstanceManager.apply(instance1));
        assertTrue(downInstanceManager.apply(instance2));
    }

    @Test
    public void testExpiration() throws Exception {
        ServiceInstance<Void> instance1 =
                ServiceInstance.<Void>builder().name("hey").id("1").build();
        ServiceInstance<Void> instance2 =
                ServiceInstance.<Void>builder().name("hey").id("2").build();

        DownInstanceManager<Void> downInstanceManager = new DownInstanceManager<Void>(debugDownInstancePolicy);

        downInstanceManager.add(instance1);
        assertFalse(downInstanceManager.apply(instance1));
        assertTrue(downInstanceManager.apply(instance2));

        Thread.sleep(debugDownInstancePolicy.getTimeoutMs());

        assertTrue(downInstanceManager.apply(instance1));
        assertTrue(downInstanceManager.apply(instance2));
    }
}
