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
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CompressionProvider;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.Test;

public class TestCompression extends BaseClassForTests {
    @Test
    public void testCompressionProvider() throws Exception {
        final byte[] data = "here's a string".getBytes();

        final AtomicInteger compressCounter = new AtomicInteger();
        final AtomicInteger decompressCounter = new AtomicInteger();
        CompressionProvider compressionProvider = new CompressionProvider() {
            @Override
            public byte[] compress(String path, byte[] data) throws Exception {
                compressCounter.incrementAndGet();

                byte[] bytes = new byte[data.length * 2];
                System.arraycopy(data, 0, bytes, 0, data.length);
                System.arraycopy(data, 0, bytes, data.length, data.length);
                return bytes;
            }

            @Override
            public byte[] decompress(String path, byte[] compressedData) throws Exception {
                decompressCounter.incrementAndGet();

                byte[] bytes = new byte[compressedData.length / 2];
                System.arraycopy(compressedData, 0, bytes, 0, bytes.length);
                return bytes;
            }
        };

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .compressionProvider(compressionProvider)
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .build();
        try {
            client.start();

            client.create().compressed().creatingParentsIfNeeded().forPath("/a/b/c", data);

            assertNotEquals(data, client.getData().forPath("/a/b/c"));
            assertEquals(data.length, client.getData().decompressed().forPath("/a/b/c").length);
        } finally {
            CloseableUtils.closeQuietly(client);
        }

        assertEquals(compressCounter.get(), 1);
        assertEquals(decompressCounter.get(), 1);
    }

    @Test
    public void testSetData() throws Exception {
        final byte[] data = "here's a string".getBytes();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();

            client.create().creatingParentsIfNeeded().forPath("/a/b/c", data);
            assertArrayEquals(data, client.getData().forPath("/a/b/c"));

            client.setData().compressed().forPath("/a/b/c", data);
            assertEquals(data.length, client.getData().decompressed().forPath("/a/b/c").length);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSetDataGlobalCompression() throws Exception {
        final byte[] data = "here's a string".getBytes();
        final byte[] gzipedData = GzipCompressionProvider.doCompress(data);

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1))
                .enableCompression()
                .build();
        try {
            client.start();

            // Create with explicit compression
            client.create().compressed().creatingParentsIfNeeded().forPath("/a/b/c", data);
            assertArrayEquals(data, client.getData().decompressed().forPath("/a/b/c"));
            assertArrayEquals(data, client.getData().forPath("/a/b/c"));
            assertArrayEquals(gzipedData, client.getData().undecompressed().forPath("/a/b/c"));
            assertEquals(
                    gzipedData.length, client.checkExists().forPath("/a/b/c").getDataLength());

            // Create explicitly without compression
            client.delete().forPath("/a/b/c");
            client.create().uncompressed().creatingParentsIfNeeded().forPath("/a/b/c", data);
            assertArrayEquals(data, client.getData().undecompressed().forPath("/a/b/c"));
            assertThrows(
                    ZipException.class, () -> client.getData().decompressed().forPath("/a/b/c"));
            assertThrows(ZipException.class, () -> client.getData().forPath("/a/b/c"));
            assertEquals(data.length, client.checkExists().forPath("/a/b/c").getDataLength());

            // Create with implicit (global) compression
            client.delete().forPath("/a/b/c");
            client.create().creatingParentsIfNeeded().forPath("/a/b/c", data);
            assertArrayEquals(data, client.getData().decompressed().forPath("/a/b/c"));
            assertArrayEquals(data, client.getData().forPath("/a/b/c"));
            assertArrayEquals(gzipedData, client.getData().undecompressed().forPath("/a/b/c"));
            assertEquals(
                    gzipedData.length, client.checkExists().forPath("/a/b/c").getDataLength());

            // SetData with explicit compression
            client.setData().compressed().forPath("/a/b/c", data);
            assertArrayEquals(data, client.getData().forPath("/a/b/c"));
            assertArrayEquals(data, client.getData().decompressed().forPath("/a/b/c"));
            assertArrayEquals(gzipedData, client.getData().undecompressed().forPath("/a/b/c"));
            assertEquals(
                    gzipedData.length, client.checkExists().forPath("/a/b/c").getDataLength());

            // SetData explicitly without compression
            client.setData().uncompressed().forPath("/a/b/c", data);
            assertArrayEquals(data, client.getData().undecompressed().forPath("/a/b/c"));
            assertThrows(
                    ZipException.class, () -> client.getData().decompressed().forPath("/a/b/c"));
            assertThrows(ZipException.class, () -> client.getData().forPath("/a/b/c"));
            assertEquals(data.length, client.checkExists().forPath("/a/b/c").getDataLength());

            // SetData with implicit (global) compression
            client.setData().forPath("/a/b/c", data);
            assertArrayEquals(data, client.getData().forPath("/a/b/c"));
            assertArrayEquals(data, client.getData().decompressed().forPath("/a/b/c"));
            assertArrayEquals(gzipedData, client.getData().undecompressed().forPath("/a/b/c"));
            assertEquals(
                    gzipedData.length, client.checkExists().forPath("/a/b/c").getDataLength());
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testSimple() throws Exception {
        final byte[] data = "here's a string".getBytes();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        try {
            client.start();

            client.create().compressed().creatingParentsIfNeeded().forPath("/a/b/c", data);

            assertNotEquals(data, client.getData().forPath("/a/b/c"));
            assertEquals(data.length, client.getData().decompressed().forPath("/a/b/c").length);
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }
}
