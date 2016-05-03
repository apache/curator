package org.apache.curator.framework.imps;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.schema.SchemaKey;
import org.apache.curator.framework.schema.SchemaSet;
import org.apache.curator.framework.schema.SchemaSetLoader;
import org.apache.curator.framework.schema.SchemaViolation;
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
