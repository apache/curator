package org.apache.curator.x.crimps.async.imps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.Watchable;
import org.apache.curator.x.crimps.async.AsyncGetChildrenBuilder;
import org.apache.curator.x.crimps.async.AsyncPathable;
import org.apache.zookeeper.data.Stat;
import java.util.List;

abstract class AsyncGetChildrenBuilderImpl<T> implements AsyncGetChildrenBuilder<T>
{
    private final CuratorFramework client;
    private final UnhandledErrorListener unhandledErrorListener;
    private Stat stat;

    public AsyncGetChildrenBuilderImpl(CuratorFramework client, UnhandledErrorListener unhandledErrorListener)
    {
        this.client = client;
        this.unhandledErrorListener = unhandledErrorListener;
    }

    @Override
    public AsyncPathable<T> storingStatIn(Stat stat)
    {
        this.stat = stat;
        return this;
    }

    @Override
    public T forPath(String path)
    {
        GetChildrenBuilder builder1 = client.getChildren();
        Watchable<? extends Pathable<List<String>>> builder2 = (stat != null) ? builder1.storingStatIn(stat) : builder1;    // TODO
        return finish(builder1, path);
    }

    protected abstract T finish(BackgroundPathable<List<String>> builder, String path);
}
