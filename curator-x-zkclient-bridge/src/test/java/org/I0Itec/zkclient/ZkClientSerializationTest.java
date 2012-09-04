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
package org.I0Itec.zkclient;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.I0Itec.zkclient.testutil.ZkTestSystem;
import org.junit.Rule;
import org.junit.Test;

public class ZkClientSerializationTest {

    @Rule
    public ZkTestSystem _zk = ZkTestSystem.getInstance();

    @Test
    public void testBytes() throws Exception {
        ZkClient zkClient = ZkTestSystem.createZkClient(_zk.getZkServerAddress());
        zkClient.setZkSerializer(new BytesPushThroughSerializer());
        byte[] bytes = new byte[100];
        new Random().nextBytes(bytes);
        zkClient.createPersistent("/a", bytes);
        byte[] readBytes = zkClient.readData("/a");
        assertArrayEquals(bytes, readBytes);
    }

    @Test
    public void testSerializables() throws Exception {
        ZkClient zkClient = ZkTestSystem.createZkClient(_zk.getZkServerAddress());
        zkClient.setZkSerializer(new SerializableSerializer());
        String data = "hello world";
        zkClient.createPersistent("/a", data);
        String readData = zkClient.readData("/a");
        assertEquals(data, readData);
    }
}
