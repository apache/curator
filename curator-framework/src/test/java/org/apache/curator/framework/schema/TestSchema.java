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
package org.apache.curator.framework.schema;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;

public class TestSchema extends BaseClassForTests
{
    @Test
    public void testBasics() throws Exception
    {
        SchemaSet schemaSet = loadSchemaSet("schema1.json");
        CuratorFramework client = newClient(schemaSet);
        try
        {
            client.start();

            try
            {
                client.create().creatingParentsIfNeeded().forPath(schemaSet.getNamedSchema(SchemaKey.named("test")).getRawPath());
                Assert.fail("Should've violated schema");
            }
            catch ( SchemaViolation dummy )
            {
                // expected
            }

            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/a/b/c");
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testMulti() throws Exception
    {
        SchemaSet schemaSet = loadSchemaSet("schema2.json");
        CuratorFramework client = newClient(schemaSet);
        try
        {
            client.start();

            try
            {
                client.create().creatingParentsIfNeeded().forPath("/a/b/c");
                Assert.fail("Should've violated schema: test");
            }
            catch ( SchemaViolation dummy )
            {
                // expected
            }

            try
            {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/a/b/c/d/e");
                Assert.fail("Should've violated schema: test2");
            }
            catch ( SchemaViolation dummy )
            {
                // expected
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    private CuratorFramework newClient(SchemaSet schemaSet)
    {
        return CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1))
            .schemaSet(schemaSet)
            .build();
    }

    private SchemaSet loadSchemaSet(String name) throws IOException
    {
        String json = Resources.toString(Resources.getResource(name), Charsets.UTF_8);
        return new SchemaSetLoader(json, null).toSchemaSet(true);
    }
}
