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
import com.google.common.collect.Lists;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TestLocalIpFilter {
    @Test
    public void testFilterEverything() throws SocketException {
        LocalIpFilter localIpFilter = ServiceInstanceBuilder.getLocalIpFilter();
        try {
            ServiceInstanceBuilder.setLocalIpFilter(new LocalIpFilter() {
                @Override
                public boolean use(NetworkInterface networkInterface, InetAddress address) throws SocketException {
                    return false;
                }
            });

            List<InetAddress> allLocalIPs = Lists.newArrayList(ServiceInstanceBuilder.getAllLocalIPs());
            assertEquals(allLocalIPs.size(), 0);
        } finally {
            ServiceInstanceBuilder.setLocalIpFilter(localIpFilter);
        }

        List<InetAddress> allLocalIPs = Lists.newArrayList(ServiceInstanceBuilder.getAllLocalIPs());
        assertTrue(allLocalIPs.size() > 0);
    }
}
