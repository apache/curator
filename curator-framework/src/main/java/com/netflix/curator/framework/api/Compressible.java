package com.netflix.curator.framework.api;

public interface Compressible<T>
{
    /**
     * Cause the data to be compressed using the configured compression provider
     *
     * @return this
     */
    public T compressed();
}
