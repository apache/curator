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


package org.apache.curator.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Test QuorumConfigBuilder
 */
public class TestQuorumConfigBuilder {

    @Test
    public void testCustomProperties() throws Exception {
        Map<String,Object> customProperties = new HashMap<String,Object>();
        customProperties.put("authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        customProperties.put("kerberos.removeHostFromPrincipal", "true");
        customProperties.put("kerberos.removeRealmFromPrincipal", "true");
        InstanceSpec spec = new InstanceSpec(null, -1, -1, -1, true, 1,-1, -1,customProperties);
        TestingServer server = new TestingServer(spec, true);
        try {
            assertEquals("org.apache.zookeeper.server.auth.SASLAuthenticationProvider", System.getProperty("zookeeper.authProvider.1"));
            assertEquals("true", System.getProperty("zookeeper.kerberos.removeHostFromPrincipal"));
            assertEquals("true", System.getProperty("zookeeper.kerberos.removeRealmFromPrincipal"));
        } finally {
            server.close();
        }
    }
}