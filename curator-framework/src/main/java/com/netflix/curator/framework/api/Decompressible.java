package com.netflix.curator.framework.api;

public interface Decompressible<T>
{
    /**
     * Cause the data to be de-compressed using the configured compression provider
     *
     * @return this
     */
    public T        decompressed();
}
