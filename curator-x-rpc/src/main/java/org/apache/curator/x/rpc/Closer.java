package org.apache.curator.x.rpc;

public interface Closer<T>
{
    public void close(T thing);
}
