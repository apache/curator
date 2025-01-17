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

package org.apache.curator.framework.imps;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.zip.ZipException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.Test;

public class TestCompressionInTransactionNew extends BaseClassForTests {
    @Test
    public void testSetData() throws Exception {
        final String path = "/a";
        final byte[] data = "here's a string".getBytes();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();

            // Create uncompressed data in a transaction
            CuratorOp op = client.transactionOp().create().forPath(path, data);
            client.transaction().forOperations(op);
            assertArrayEquals(data, client.getData().forPath(path));

            // Create compressed data in transaction
            op = client.transactionOp().setData().compressed().forPath(path, data);
            client.transaction().forOperations(op);
            assertArrayEquals(data, client.getData().decompressed().forPath(path));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSetCompressedAndUncompressed() throws Exception {
        final String path1 = "/a";
        final String path2 = "/b";

        final byte[] data1 = "here's a string".getBytes();
        final byte[] data2 = "here's another string".getBytes();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();

            // Create the nodes
            CuratorOp op1 = client.transactionOp().create().compressed().forPath(path1);
            CuratorOp op2 = client.transactionOp().create().forPath(path2);
            client.transaction().forOperations(op1, op2);

            // Check they exist
            assertNotNull(client.checkExists().forPath(path1));
            assertNotNull(client.checkExists().forPath(path2));

            // Set the nodes, path1 compressed, path2 uncompressed.
            op1 = client.transactionOp().setData().compressed().forPath(path1, data1);
            op2 = client.transactionOp().setData().forPath(path2, data2);
            client.transaction().forOperations(op1, op2);

            assertNotEquals(data1, client.getData().forPath(path1));
            assertArrayEquals(data1, client.getData().decompressed().forPath(path1));

            assertArrayEquals(data2, client.getData().forPath(path2));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSimple() throws Exception {
        final String path1 = "/a";
        final String path2 = "/a/b";

        final byte[] data1 = "here's a string".getBytes();
        final byte[] data2 = "here's another string".getBytes();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();

            CuratorOp op1 = client.transactionOp().create().compressed().forPath(path1, data1);
            CuratorOp op2 = client.transactionOp().create().compressed().forPath(path2, data2);

            client.transaction().forOperations(op1, op2);

            assertNotEquals(data1, client.getData().forPath(path1));
            assertArrayEquals(data1, client.getData().decompressed().forPath(path1));

            assertNotEquals(data2, client.getData().forPath(path2));
            assertArrayEquals(data2, client.getData().decompressed().forPath(path2));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * Test the case where both uncompressed and compressed data is generated in
     * the same transaction
     * @throws Exception
     */
    @Test
    public void testCreateCompressedAndUncompressed() throws Exception {
        final String path1 = "/a";
        final String path2 = "/b";

        final byte[] data1 = "here's a string".getBytes();
        final byte[] data2 = "here's another string".getBytes();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();

            CuratorOp op1 = client.transactionOp().create().compressed().forPath(path1, data1);
            CuratorOp op2 = client.transactionOp().create().forPath(path2, data2);
            client.transaction().forOperations(op1, op2);

            assertNotEquals(data1, client.getData().forPath(path1));
            assertArrayEquals(data1, client.getData().decompressed().forPath(path1));

            assertArrayEquals(data2, client.getData().forPath(path2));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testGlobalCompression() throws Exception {
        final String path1 = "/a";
        final String path2 = "/b";

        final byte[] data1 = "here's a string".getBytes();
        final byte[] data2 = "here's another string".getBytes();
        final byte[] gzipedData1 = GzipCompressionProvider.doCompress(data1);
        final byte[] gzipedData2 = GzipCompressionProvider.doCompress(data2);

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .enableCompression()
                .build();
        try {
            client.start();

            // Create the nodes in a transaction
            // path1 is compressed (globally)
            // path2 is uncompressed (override)
            CuratorOp op1 = client.transactionOp().create().forPath(path1, data1);
            CuratorOp op2 = client.transactionOp().create().uncompressed().forPath(path2, data2);
            client.transaction().forOperations(op1, op2);

            // Check they exist
            assertNotNull(client.checkExists().forPath(path1));
            assertEquals(gzipedData1.length, client.checkExists().forPath(path1).getDataLength());
            assertNotNull(client.checkExists().forPath(path2));
            assertEquals(data2.length, client.checkExists().forPath(path2).getDataLength());
            assertArrayEquals(data1, client.getData().forPath(path1));
            assertArrayEquals(data1, client.getData().decompressed().forPath(path1));
            assertArrayEquals(gzipedData1, client.getData().undecompressed().forPath(path1));
            assertArrayEquals(data2, client.getData().undecompressed().forPath(path2));
            assertThrows(
                    ZipException.class, () -> client.getData().decompressed().forPath(path2));
            assertThrows(ZipException.class, () -> client.getData().forPath(path2));

            // Set data in transaction
            // path1 is uncompressed (override)
            // path2 is compressed (globally)
            op1 = client.transactionOp().setData().uncompressed().forPath(path1, data1);
            op2 = client.transactionOp().setData().forPath(path2, data2);
            client.transaction().forOperations(op1, op2);

            assertNotNull(client.checkExists().forPath(path1));
            assertEquals(data1.length, client.checkExists().forPath(path1).getDataLength());
            assertNotNull(client.checkExists().forPath(path2));
            assertEquals(gzipedData2.length, client.checkExists().forPath(path2).getDataLength());
            assertArrayEquals(data1, client.getData().undecompressed().forPath(path1));
            assertThrows(
                    ZipException.class, () -> client.getData().decompressed().forPath(path1));
            assertThrows(ZipException.class, () -> client.getData().forPath(path1));
            assertArrayEquals(data2, client.getData().decompressed().forPath(path2));
            assertArrayEquals(data2, client.getData().forPath(path2));
            assertArrayEquals(gzipedData2, client.getData().undecompressed().forPath(path2));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }
}
