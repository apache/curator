package org.apache.curator.x.rest.details;

public interface Closer<T>
{
    public void close(T thing);
}
