package com.netflix.curator.framework.api;

public interface CompressionProvider
{
    public byte[]       compress(String path, byte[] data) throws Exception;

    public byte[]       decompress(String path, byte[] compressedData) throws Exception;
}
