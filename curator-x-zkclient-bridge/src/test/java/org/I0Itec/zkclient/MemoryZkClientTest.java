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

import org.apache.zookeeper.CreateMode;
import org.junit.Assert;
import org.junit.Test;

public class MemoryZkClientTest extends AbstractBaseZkClientTest {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _client = new ZkClient(new InMemoryConnection());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        _client.close();
    }

    @Test
    public void testGetChildren() throws Exception {
        String path1 = "/a";
        String path2 = "/a/a";
        String path3 = "/a/a/a";

        _client.create(path1, null, CreateMode.PERSISTENT);
        _client.create(path2, null, CreateMode.PERSISTENT);
        _client.create(path3, null, CreateMode.PERSISTENT);
        Assert.assertEquals(1, _client.getChildren(path1).size());
        Assert.assertEquals(1, _client.getChildren(path2).size());
        Assert.assertEquals(0, _client.getChildren(path3).size());
    }
}
