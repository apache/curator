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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.GZIPOutputStream;

public class TestGzipCompressionProvider
{
    @Test
    public void testSimple() throws IOException
    {
        GzipCompressionProvider provider = new GzipCompressionProvider();
        byte[] data = "Hello, world!".getBytes();
        byte[] compressedData = provider.compress(null, data);
        byte[] jdkCompressedData = jdkCompress(data);
        assertArrayEquals(compressedData, jdkCompressedData);
        byte[] decompressedData = provider.decompress(null, compressedData);
        assertArrayEquals(decompressedData, data);
    }

    @Test
    public void testEmpty() throws IOException
    {
        GzipCompressionProvider provider = new GzipCompressionProvider();
        byte[] compressedData = provider.compress(null, new byte[0]);
        byte[] compressedData2 = GzipCompressionProvider.doCompress(new byte[0]);
        byte[] jdkCompress = jdkCompress(new byte[0]);
        // Ensures GzipCompressionProvider.COMPRESSED_EMPTY_BYTES value is valid
        assertArrayEquals(compressedData, compressedData2);
        assertArrayEquals(compressedData, jdkCompress);
        byte[] decompressedData = provider.decompress(null, compressedData);
        assertEquals(0, decompressedData.length);
    }

    /**
     * This test ensures that in the face of corrupt data, specifically IOException is thrown, rather some other kind
     * of runtime exception. Users of {@link GzipCompressionProvider#decompress(String, byte[])} may depend on this.
     */
    @Test
    public void testDecompressCorrupt()
    {
        GzipCompressionProvider provider = new GzipCompressionProvider();
        try {
            provider.decompress(null, new byte[100]);
            fail("Expected IOException");
        } catch (IOException ignore) {
            // expected
        }
        byte[] compressedData = provider.compress(null, new byte[0]);
        for (int i = 0; i < compressedData.length; i++)
        {
            try {
                provider.decompress(null, Arrays.copyOf(compressedData, i));
            } catch (IOException ignore) {
                // expected
            }
            for (int change = 1; change < 256; change++)
            {
                byte b = compressedData[i];
                compressedData[i] = (byte) (b + change);
                try {
                    provider.decompress(null, compressedData);
                    // No exception is OK
                } catch (IOException ignore) {
                    // expected
                }
                // reset value back
                compressedData[i] = b;
            }
        }
    }

    @Test
    public void smokeTestRandomDataWithJdk() throws IOException
    {
        GzipCompressionProvider provider = new GzipCompressionProvider();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int len = 1; len < 100; len++)
        {
            byte[] data = new byte[len];
            for (int i = 0; i < 100; i++) {
                byte[] compressedData = provider.compress(null, data);
                byte[] jdkCompressedData = jdkCompress(data);
                assertArrayEquals(compressedData, jdkCompressedData);
                byte[] decompressedData = provider.decompress(null, compressedData);
                assertArrayEquals(decompressedData, data);
                // in the end of the iteration to test empty array first
                random.nextBytes(data);
            }
        }
    }

    private static byte[] jdkCompress(byte[] data) throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(bytes)) {
            out.write(data);
            out.finish();
        }
        return bytes.toByteArray();
    }
}
