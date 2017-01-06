package org.apache.curator.x.crimps.async.imps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetConfigBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.x.crimps.async.AsyncGetChildrenBuilder;
import org.apache.curator.x.crimps.async.WatchedAsyncCuratorFramework;
import org.apache.curator.x.crimps.async.details.AsyncCrimps;
import org.apache.curator.x.crimps.async.details.Crimped;
import java.util.List;

public class WatchedAsyncCuratorFrameworkImpl implements WatchedAsyncCuratorFramework
{
    private final CuratorFramework client;
    private final AsyncCrimps async;
    private final UnhandledErrorListener unhandledErrorListener;

    public WatchedAsyncCuratorFrameworkImpl(CuratorFramework client, AsyncCrimps async, UnhandledErrorListener unhandledErrorListener)
    {
        this.client = client;
        this.async = async;
        this.unhandledErrorListener = unhandledErrorListener;
    }

    @Override
    public ExistsBuilder checkExists()
    {
        return null;
    }

    @Override
    public GetDataBuilder getData()
    {
        return null;
    }

    @Override
    public AsyncGetChildrenBuilder<Crimped<List<String>>> getChildren()
    {
        return new AsyncGetChildrenBuilderImpl<Crimped<List<String>>>(client, unhandledErrorListener)
        {
            @Override
            protected Crimped<List<String>> finish(BackgroundPathable<List<String>> builder, String path)
            {
                return null;    // TODO
            }
        };
    }

    @Override
    public GetConfigBuilder getConfig()
    {
        return null;
    }
}
