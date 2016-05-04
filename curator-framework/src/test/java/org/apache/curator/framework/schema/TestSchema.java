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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class TestSchema extends BaseClassForTests
{
    @Test
    public void testBasics() throws Exception
    {
        SchemaSet schemaSet = loadSchemaSet("schema1.json", null);
        Schema schema = schemaSet.getNamedSchema("test");
        Assert.assertNotNull(schema);
        Map<String, String> expectedMetadata = Maps.newHashMap();
        expectedMetadata.put("one", "1");
        expectedMetadata.put("two", "2");
        Assert.assertEquals(schema.getMetadata(), expectedMetadata);

        CuratorFramework client = newClient(schemaSet);
        try
        {
            client.start();

            try
            {
                String rawPath = schema.getRawPath();
                Assert.assertEquals(rawPath, "/a/b/c");
                client.create().creatingParentsIfNeeded().forPath(rawPath);
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
    public void testSchemaValidator() throws Exception
    {
        final SchemaValidator schemaValidator = new SchemaValidator()
        {
            @Override
            public boolean isValid(Schema schema, String path, byte[] data, List<ACL> acl)
            {
                return data.length > 0;
            }
        };
        SchemaSetLoader.SchemaValidatorMapper schemaValidatorMapper = new SchemaSetLoader.SchemaValidatorMapper()
        {
            @Override
            public SchemaValidator getSchemaValidator(String name)
            {
                return schemaValidator;
            }
        };
        SchemaSet schemaSet = loadSchemaSet("schema3.json", schemaValidatorMapper);
        CuratorFramework client = newClient(schemaSet);
        try
        {
            client.start();

            try
            {
                client.create().forPath("/test", new byte[0]);
                Assert.fail("Should've violated schema");
            }
            catch ( SchemaViolation dummy )
            {
                // expected
            }

            client.create().forPath("/test", "good".getBytes());
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testMulti() throws Exception
    {
        SchemaSet schemaSet = loadSchemaSet("schema2.json", null);
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

    @Test
    public void testTransaction() throws Exception
    {
        final SchemaValidator schemaValidator = new SchemaValidator()
        {
            @Override
            public boolean isValid(Schema schema, String path, byte[] data, List<ACL> acl)
            {
                return data.length > 0;
            }
        };
        SchemaSetLoader.SchemaValidatorMapper schemaValidatorMapper = new SchemaSetLoader.SchemaValidatorMapper()
        {
            @Override
            public SchemaValidator getSchemaValidator(String name)
            {
                return schemaValidator;
            }
        };
        SchemaSet schemaSet = loadSchemaSet("schema4.json", schemaValidatorMapper);
        CuratorFramework client = newClient(schemaSet);
        try
        {
            client.start();

            CuratorOp createAPersistent = client.transactionOp().create().forPath("/a");
            CuratorOp createAEphemeral = client.transactionOp().create().withMode(CreateMode.EPHEMERAL).forPath("/a");
            CuratorOp deleteA = client.transactionOp().delete().forPath("/a");
            CuratorOp createBEmptyData = client.transactionOp().create().forPath("/b", new byte[0]);
            CuratorOp createBWithData = client.transactionOp().create().forPath("/b", new byte[10]);
            CuratorOp setBEmptyData = client.transactionOp().setData().forPath("/b", new byte[0]);
            CuratorOp setBWithData = client.transactionOp().setData().forPath("/b", new byte[10]);

            try
            {
                client.transaction().forOperations(createAPersistent, createAEphemeral);
                Assert.fail("Should've violated schema");
            }
            catch ( SchemaViolation dummy )
            {
                // expected
            }
            client.transaction().forOperations(createAEphemeral);

            try
            {
                client.transaction().forOperations(deleteA);
                Assert.fail("Should've violated schema");
            }
            catch ( SchemaViolation dummy )
            {
                // expected
            }

            try
            {
                client.transaction().forOperations(createBEmptyData);
                Assert.fail("Should've violated schema");
            }
            catch ( SchemaViolation dummy )
            {
                // expected
            }
            client.transaction().forOperations(createBWithData);

            try
            {
                client.transaction().forOperations(setBEmptyData);
                Assert.fail("Should've violated schema");
            }
            catch ( SchemaViolation dummy )
            {
                // expected
            }
            client.transaction().forOperations(setBWithData);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testOrdering() throws Exception
    {
        SchemaSet schemaSet = loadSchemaSet("schema5.json", null);
        CuratorFramework client = newClient(schemaSet);
        try
        {
            client.start();

            try
            {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/exact/match");
                Assert.fail("Should've violated schema");
            }
            catch ( SchemaViolation dummy )
            {
                // expected
            }

            try
            {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/exact/foo/bar");
                Assert.fail("Should've violated schema");
            }
            catch ( SchemaViolation dummy )
            {
                // expected
            }

            try
            {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/exact/other/bar");
                Assert.fail("Should've violated schema");
            }
            catch ( SchemaViolation dummy )
            {
                // expected
            }

            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/exact/match");   // schema "1"
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/exact/other/thing");   // schema "2"
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/exact/foo/bar");   // schema "3"
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Override
    protected boolean enabledSessionExpiredStateAware()
    {
        return true;
    }

    private CuratorFramework newClient(SchemaSet schemaSet)
    {
        return CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .retryPolicy(new RetryOneTime(1))
            .schemaSet(schemaSet)
            .build();
    }

    private SchemaSet loadSchemaSet(String name, SchemaSetLoader.SchemaValidatorMapper schemaValidatorMapper) throws IOException
    {
        String json = Resources.toString(Resources.getResource(name), Charsets.UTF_8);
        return new SchemaSetLoader(json, schemaValidatorMapper).toSchemaSet(true);
    }
}
