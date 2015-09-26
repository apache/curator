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
package org.apache.curator.framework.imps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCompressionInTransactionNew extends BaseClassForTests
{
    @Test
    public void testSetData() throws Exception
    {
        final String path = "/a";
        final byte[]            data = "here's a string".getBytes();

        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            //Create uncompressed data in a transaction
            CuratorOp op = client.transactionOp().create().forPath(path, data);
            client.transaction().forOperations(op);
            Assert.assertEquals(data, client.getData().forPath(path));

            //Create compressed data in transaction
            op = client.transactionOp().setData().compressed().forPath(path, data);
            client.transaction().forOperations(op);
            Assert.assertEquals(data, client.getData().decompressed().forPath(path));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
    
    @Test
    public void testSetCompressedAndUncompressed() throws Exception
    {
        final String path1 = "/a";
        final String path2 = "/b";
        
        final byte[]            data1 = "here's a string".getBytes();
        final byte[]            data2 = "here's another string".getBytes();

        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            //Create the nodes
            CuratorOp op1 = client.transactionOp().create().compressed().forPath(path1);
            CuratorOp op2 = client.transactionOp().create().forPath(path2);
            client.transaction().forOperations(op1, op2);

            //Check they exist
            Assert.assertNotNull(client.checkExists().forPath(path1));
            Assert.assertNotNull(client.checkExists().forPath(path2));
            
            //Set the nodes, path1 compressed, path2 uncompressed.
            op1 = client.transactionOp().setData().compressed().forPath(path1, data1);
            op2 = client.transactionOp().setData().forPath(path2, data2);
            client.transaction().forOperations(op1, op2);
            
            Assert.assertNotEquals(data1, client.getData().forPath(path1));
            Assert.assertEquals(data1, client.getData().decompressed().forPath(path1));
      
            Assert.assertEquals(data2, client.getData().forPath(path2));            
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }    
    
    @Test
    public void testSimple() throws Exception
    {
        final String path1 = "/a";
        final String path2 = "/a/b";
        
        final byte[]            data1 = "here's a string".getBytes();
        final byte[]            data2 = "here's another string".getBytes();

        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            CuratorOp op1 = client.transactionOp().create().compressed().forPath(path1, data1);
            CuratorOp op2 = client.transactionOp().create().compressed().forPath(path2, data2);

            client.transaction().forOperations(op1, op2);

            Assert.assertNotEquals(data1, client.getData().forPath(path1));
            Assert.assertEquals(data1, client.getData().decompressed().forPath(path1));
            
            Assert.assertNotEquals(data2, client.getData().forPath(path2));
            Assert.assertEquals(data2, client.getData().decompressed().forPath(path2));            
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }    
    
    /**
     * Test the case where both uncompressed and compressed data is generated in
     * the same transaction
     * @throws Exception
     */
    @Test
    public void testCreateCompressedAndUncompressed() throws Exception
    {
        final String path1 = "/a";
        final String path2 = "/b";
        
        final byte[]            data1 = "here's a string".getBytes();
        final byte[]            data2 = "here's another string".getBytes();

        CuratorFramework        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try
        {
            client.start();

            CuratorOp op1 = client.transactionOp().create().compressed().forPath(path1, data1);
            CuratorOp op2 = client.transactionOp().create().forPath(path2, data2);
            client.transaction().forOperations(op1, op2);

            Assert.assertNotEquals(data1, client.getData().forPath(path1));
            Assert.assertEquals(data1, client.getData().decompressed().forPath(path1));
      
            Assert.assertEquals(data2, client.getData().forPath(path2));            
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }
}
