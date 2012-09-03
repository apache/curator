/**
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.I0Itec.zkclient.util;

import com.netflix.curator.test.TestingServer;
import junit.framework.TestCase;

import org.I0Itec.zkclient.TestUtil;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.testutil.ZkPathUtil;
import org.I0Itec.zkclient.testutil.ZkTestSystem;

public class ZkPathUtilTest extends TestCase {

    protected TestingServer _zkServer;
    protected ZkClient _client;

    public void testToString() throws Exception {
        _zkServer = new TestingServer(4711);
        _client = ZkTestSystem.createZkClient("localhost:4711");
        final String file1 = "/files/file1";
        final String file2 = "/files/file2";
        final String file3 = "/files/file2/file3";
        _client.createPersistent(file1, true);
        _client.createPersistent(file2, true);
        _client.createPersistent(file3, true);

        String stringRepresentation = ZkPathUtil.toString(_client);
        System.out.println(stringRepresentation);
        System.out.println("-------------------------");
        assertTrue(stringRepresentation.contains("file1"));
        assertTrue(stringRepresentation.contains("file2"));
        assertTrue(stringRepresentation.contains("file3"));

        // path filtering
        stringRepresentation = ZkPathUtil.toString(_client, "/", new ZkPathUtil.PathFilter() {
            @Override
            public boolean showChilds(String path) {
                return !file2.equals(path);
            }
        });
        assertTrue(stringRepresentation.contains("file1"));
        assertTrue(stringRepresentation.contains("file2"));
        assertFalse(stringRepresentation.contains("file3"));

        // start path
        stringRepresentation = ZkPathUtil.toString(_client, file2, ZkPathUtil.PathFilter.ALL);
        assertFalse(stringRepresentation.contains("file1"));
        assertTrue(stringRepresentation.contains("file2"));
        assertTrue(stringRepresentation.contains("file3"));

        _zkServer.close();
    }

    public void testLeadingZeros() throws Exception {
        assertEquals("0000000001", ZkPathUtil.leadingZeros(1, 10));
    }
}
