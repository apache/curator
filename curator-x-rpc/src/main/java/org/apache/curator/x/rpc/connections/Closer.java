package org.apache.curator.x.rpc.connections;

public interface Closer<T>
{
    public void close(T thing);
}
