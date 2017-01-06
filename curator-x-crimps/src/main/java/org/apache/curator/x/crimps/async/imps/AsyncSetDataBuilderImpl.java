package org.apache.curator.x.crimps.async.imps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.x.crimps.async.AsyncPathAndBytesable;
import org.apache.curator.x.crimps.async.AsyncSetDataBuilder;
import org.apache.curator.x.crimps.async.details.AsyncCrimps;
import org.apache.zookeeper.data.Stat;
import java.util.concurrent.CompletionStage;

class AsyncSetDataBuilderImpl implements AsyncSetDataBuilder
{
    private final CuratorFramework client;
    private final AsyncCrimps async;
    private final UnhandledErrorListener unhandledErrorListener;
    private boolean compressed = false;
    private int version = -1;

    AsyncSetDataBuilderImpl(CuratorFramework client, AsyncCrimps async, UnhandledErrorListener unhandledErrorListener)
    {
        this.client = client;
        this.async = async;
        this.unhandledErrorListener = unhandledErrorListener;
    }

    @Override
    public CompletionStage<Stat> forPath(String path)
    {
        return forPath(path, null);
    }

    @Override
    public CompletionStage<Stat> forPath(String path, byte[] data)
    {
        SetDataBuilder builder1 = client.setData();
        BackgroundPathAndBytesable<Stat> builder2 = (version >= 0) ? builder1.withVersion(version) : builder1;
        // compressed ? builder2.compressed() : builder2; TODO compressed with version
        if ( data != null )
        {
            return async.stat(builder2).forPath(path, data);
        }
        return async.stat(builder2).forPath(path);
    }

    @Override
    public AsyncPathAndBytesable<CompletionStage<Stat>> compressed()
    {
        compressed = true;
        return this;
    }

    @Override
    public AsyncPathAndBytesable<CompletionStage<Stat>> compressedWithVersion(int version)
    {
        compressed = true;
        this.version = version;
        return this;
    }

    @Override
    public AsyncPathAndBytesable<CompletionStage<Stat>> withVersion(int version)
    {
        this.version = version;
        return this;
    }
}
