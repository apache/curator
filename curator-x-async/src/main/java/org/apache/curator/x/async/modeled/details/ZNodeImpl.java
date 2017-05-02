package org.apache.curator.x.async.modeled.details;

import org.apache.curator.x.async.modeled.cached.ZNode;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.data.Stat;
import java.util.Objects;

public class ZNodeImpl<T> implements ZNode<T>
{
    private final ZPath path;
    private final Stat stat;
    private final T model;

    public ZNodeImpl(ZPath path, Stat stat, T model)
    {
        this.path = Objects.requireNonNull(path, "path cannot be null");
        this.stat = Objects.requireNonNull(stat, "stat cannot be null");
        this.model = Objects.requireNonNull(model, "model cannot be null");
    }

    @Override
    public ZPath path()
    {
        return path;
    }

    @Override
    public Stat stat()
    {
        return stat;
    }

    @Override
    public T model()
    {
        return model;
    }
}
