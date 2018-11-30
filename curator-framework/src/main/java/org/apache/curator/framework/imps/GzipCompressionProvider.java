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

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.api.CompressionProvider;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.*;

public class GzipCompressionProvider implements CompressionProvider
{
    // This class re-implements java.util.zip.GZIPInputStream and GZIPOutputStream functionality to avoid
    // creation many finalized Deflater and Inflater objects on heap (see
    // https://issues.apache.org/jira/browse/CURATOR-487). Even when Curator's minimum supported Java version becomes
    // no less than Java 12, where finalize() methods are removed in Deflater and Inflater classes and instead they
    // are phantom-referenced via Cleaner, it still makes sense to avoid GZIPInputStream and GZIPOutputStream because
    // phantom references are also not entirely free for GC algorithms, and also to allocate less garbage and make
    // less unnecessary data copies.

    private static final int MAX_SAFE_JAVA_BYTE_ARRAY_SIZE = Integer.MAX_VALUE - 128;

    /** GZIP header magic number. */
    private static final int GZIP_MAGIC = 0x8b1f;

    /** See {@code java.util.zip.GZIPOutputStream.writeHeader()} */
    private static final byte[] GZIP_HEADER = new byte[] {
            (byte) GZIP_MAGIC,        // Magic number (byte 0)
            (byte) (GZIP_MAGIC >> 8), // Magic number (byte 1)
            Deflater.DEFLATED,        // Compression method (CM)
            0,                        // Flags (FLG)
            0,                        // Modification time MTIME (byte 0)
            0,                        // Modification time MTIME (byte 1)
            0,                        // Modification time MTIME (byte 2)
            0,                        // Modification time MTIME (byte 3)
            0,                        // Extra flags (XFLG)
            0                         // Operating system (OS)
    };

    /** GZip flags, {@link #GZIP_HEADER}'s 4th byte */
    private static final int FHCRC = 1 << 1;
    private static final int FEXTRA = 1 << 2;
    private static final int FNAME  = 1 << 3;
    private static final int FCOMMENT = 1 << 4;

    private static final int GZIP_HEADER_SIZE = GZIP_HEADER.length;

    /** 32-bit CRC and uncompressed data size */
    private static final int GZIP_TRAILER_SIZE = Integer.BYTES + Integer.BYTES;

    /** DEFLATE doesn't produce shorter compressed data */
    private static final int MIN_COMPRESSED_DATA_SIZE = 2;

    /**
     * Since Deflaters and Inflaters are acquired and returned to the pools in try-finally blocks that are free of
     * blocking calls themselves, it's not expected that the number of objects in the pools could exceed the number of
     * hardware threads on the machine much. Therefore it's accepted to have simple "ever-growing" (in fact, no) pools
     * of strongly-referenced objects.
     */
    private static final ConcurrentLinkedQueue<Deflater> DEFLATER_POOL = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<Inflater> INFLATER_POOL = new ConcurrentLinkedQueue<>();

    /** The value verified in GzipCompressionProviderTest.testEmpty() */
    private static final byte[] COMPRESSED_EMPTY_BYTES = new byte[] {
            31, -117, 8, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0
    };

    private static Deflater acquireDeflater()
    {
        Deflater deflater = DEFLATER_POOL.poll();
        if ( deflater == null )
        {
            // Using the same settings as in GZIPOutputStream constructor
            deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        }
        return deflater;
    }

    private static Inflater acquireInflater()
    {
        Inflater inflater = INFLATER_POOL.poll();
        if ( inflater == null )
        {
            // Using the same nowrap setting as GZIPInputStream constructor
            inflater = new Inflater(true);
        }
        return inflater;
    }

    @Override
    public byte[] compress(String path, byte[] data)
    {
        if ( data.length == 0 )
        {
            // clone() because clients could update the array
            return COMPRESSED_EMPTY_BYTES.clone();
        }
        return doCompress(data);
    }

    @VisibleForTesting
    static byte[] doCompress(byte[] data)
    {
        byte[] result = Arrays.copyOf(GZIP_HEADER, conservativeGZippedSizeEstimate(data.length));
        Deflater deflater = acquireDeflater();
        try {
            deflater.setInput(data);
            deflater.finish();
            int offset = GZIP_HEADER_SIZE;
            while ( true )
            {
                int available = result.length - GZIP_TRAILER_SIZE - offset;
                int numCompressedBytes = deflater.deflate(result, offset, available);
                offset += numCompressedBytes;
                if ( deflater.finished() )
                {
                    break;
                }
                int newResultLength = result.length + (result.length / 2);
                result = Arrays.copyOf(result, newResultLength);
            }
            // Write GZip trailer
            CRC32 crc = new CRC32();
            crc.update(data, 0, data.length);
            writeLittleEndianInt(result, offset, (int) crc.getValue());
            writeLittleEndianInt(result, offset + 4, data.length);
            int endOffset = offset + GZIP_TRAILER_SIZE;
            if ( result.length != endOffset )
            {
                result = Arrays.copyOf(result, endOffset);
            }
            return result;
        } finally {
            deflater.reset();
            DEFLATER_POOL.add(deflater);
        }
    }

    private static int conservativeGZippedSizeEstimate(int dataSize)
    {
        int conservativeCompressedDataSizeEstimate;
        if ( dataSize < 512 )
        {
            // Assuming DEFLATE doesn't compress small data well
            conservativeCompressedDataSizeEstimate = Math.max(dataSize, MIN_COMPRESSED_DATA_SIZE);
        }
        else
        {
            // Assuming pretty bad 2:1 compression ratio
            conservativeCompressedDataSizeEstimate = Math.max(512, dataSize / 2);
        }
        return GZIP_HEADER_SIZE + conservativeCompressedDataSizeEstimate + GZIP_TRAILER_SIZE;
    }

    private static void writeLittleEndianInt(byte[] b, int offset, int v)
    {
        b[offset] = (byte) v;
        b[offset + 1] = (byte) (v >> 8);
        b[offset + 2] = (byte) (v >> 16);
        b[offset + 3] = (byte) (v >> 24);
    }

    @Override
    public byte[] decompress(String path, byte[] gzippedDataBytes) throws IOException {
        if ( Arrays.equals(gzippedDataBytes, COMPRESSED_EMPTY_BYTES) )
        {
            // Allocating a new array instead of creating a static constant because clients may somehow depend on the
            // identity of the returned arrays
            return new byte[0];
        }
        ByteBuffer gzippedData = ByteBuffer.wrap(gzippedDataBytes);
        gzippedData.order(ByteOrder.LITTLE_ENDIAN);
        int headerSize = readGzipHeader(gzippedData);
        if ( gzippedDataBytes.length < headerSize + MIN_COMPRESSED_DATA_SIZE + GZIP_TRAILER_SIZE )
        {
            throw new EOFException("Too short GZipped data");
        }
        int compressedDataSize = gzippedDataBytes.length - headerSize - GZIP_TRAILER_SIZE;
        // Assuming 3:1 compression ratio. Intentionally a more generous estimation than in
        // conservativeGZippedSizeEstimate() to reduce the probability of result array reallocation.
        int initialResultLength = (int) Math.min(compressedDataSize * 3L, MAX_SAFE_JAVA_BYTE_ARRAY_SIZE);
        byte[] result = new byte[initialResultLength];
        Inflater inflater = acquireInflater();
        try {
            inflater.setInput(gzippedDataBytes, headerSize, compressedDataSize);
            CRC32 crc = new CRC32();
            int offset = 0;
            while (true)
            {
                int numDecompressedBytes;
                try {
                    numDecompressedBytes = inflater.inflate(result, offset, result.length - offset);
                } catch (DataFormatException e) {
                    String s = e.getMessage();
                    throw new ZipException(s != null ? s : "Invalid ZLIB data format");
                }
                crc.update(result, offset, numDecompressedBytes);
                offset += numDecompressedBytes;
                if ( inflater.finished() || inflater.needsDictionary() )
                {
                    break;
                }
                // Just calling inflater.needsInput() doesn't work as expected, apparently it doesn't uphold it's own
                // contract and could have needsInput() == true if numDecompressedBytes != 0 and that just means that
                // there is not enough space in the result array
                else if ( numDecompressedBytes == 0 && inflater.needsInput() )
                {
                    throw new ZipException("Corrupt GZipped data");
                }
                // Inflater's contract doesn't say whether it's able to be finished() without returning 0 from inflate()
                // call, so the additional `numDecompressedBytes == 0` condition ensures that we did another cycle and
                // definitely need to inflate some more bytes.
                if ( result.length == MAX_SAFE_JAVA_BYTE_ARRAY_SIZE && numDecompressedBytes == 0 )
                {
                    throw new OutOfMemoryError("Unable to uncompress that much data into a single byte[] array");
                }
                int newResultLength =
                        (int) Math.min((long) result.length + (result.length / 2), MAX_SAFE_JAVA_BYTE_ARRAY_SIZE);
                if ( result.length != newResultLength )
                {
                    result = Arrays.copyOf(result, newResultLength);
                }
            }
            if ( inflater.getRemaining() != 0 )
            {
                throw new ZipException("Expected just one GZip block, without garbage in the end");
            }
            int checksum = gzippedData.getInt(gzippedDataBytes.length - GZIP_TRAILER_SIZE);
            int numUncompressedBytes = gzippedData.getInt(gzippedDataBytes.length - Integer.BYTES);
            if ( checksum != (int) crc.getValue() || numUncompressedBytes != offset )
            {
                throw new ZipException("Corrupt GZIP trailer");
            }
            if ( result.length != offset )
            {
                result = Arrays.copyOf(result, offset);
            }
            return result;
        } finally {
            inflater.reset();
            INFLATER_POOL.add(inflater);
        }
    }

    /**
     * Returns the header size
     */
    private static int readGzipHeader(ByteBuffer gzippedData) throws IOException
    {
        try {
            return doReadHeader(gzippedData);
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    private static int doReadHeader(ByteBuffer gzippedData) throws IOException {
        if ( gzippedData.getChar() != GZIP_MAGIC )
        {
            throw new ZipException("Not in GZip format");
        }
        if ( gzippedData.get() != Deflater.DEFLATED )
        {
            throw new ZipException("Unsupported compression method");
        }
        int flags = gzippedData.get();
        // Skip MTIME, XFL, and OS fields
        skip(gzippedData, Integer.BYTES + Byte.BYTES + Byte.BYTES);
        if ( (flags & FEXTRA) != 0 )
        {
            int extraBytes = gzippedData.getChar();
            skip(gzippedData, extraBytes);
        }
        if ( (flags & FNAME) != 0 )
        {
            skipZeroTerminatedString(gzippedData);
        }
        if ( (flags & FCOMMENT) != 0 )
        {
            skipZeroTerminatedString(gzippedData);
        }
        if ( (flags & FHCRC) != 0 )
        {
            CRC32 crc = new CRC32();
            crc.update(gzippedData.array(), 0, gzippedData.position());
            if ( gzippedData.getChar() != (char) crc.getValue() )
            {
                throw new ZipException("Corrupt GZIP header");
            }
        }
        return gzippedData.position();
    }

    private static void skip(ByteBuffer gzippedData, int skipBytes) throws IOException
    {
        try {
            gzippedData.position(gzippedData.position() + skipBytes);
        } catch (IllegalArgumentException e) {
            throw new EOFException();
        }
    }

    private static void skipZeroTerminatedString(ByteBuffer gzippedData)
    {
        while (gzippedData.get() != 0) {
            // loop
        }
    }
}
