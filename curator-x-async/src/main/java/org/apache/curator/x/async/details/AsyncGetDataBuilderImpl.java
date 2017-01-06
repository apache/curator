package org.apache.curator.x.async.details;

import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.GetDataBuilderImpl;
import org.apache.curator.x.async.AsyncGetDataBuilder;
import org.apache.curator.x.async.AsyncPathable;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.data.Stat;

import static org.apache.curator.x.async.details.BackgroundProcs.dataProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;

class AsyncGetDataBuilderImpl implements AsyncGetDataBuilder
{
    private final CuratorFrameworkImpl client;
    private final UnhandledErrorListener unhandledErrorListener;
    private final boolean watched;
    private boolean decompressed = false;
    private Stat stat = null;

    AsyncGetDataBuilderImpl(CuratorFrameworkImpl client, UnhandledErrorListener unhandledErrorListener, boolean watched)
    {
        this.client = client;
        this.unhandledErrorListener = unhandledErrorListener;
        this.watched = watched;
    }

    @Override
    public AsyncPathable<AsyncStage<byte[]>> decompressed()
    {
        decompressed = true;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<byte[]>> storingStatIn(Stat stat)
    {
        this.stat = stat;
        return this;
    }

    @Override
    public AsyncPathable<AsyncStage<byte[]>> decompressedStoringStatIn(Stat stat)
    {
        decompressed = true;
        this.stat = stat;
        return this;
    }

    @Override
    public AsyncStage<byte[]> forPath(String path)
    {
        BuilderCommon<byte[]> common = new BuilderCommon<>(unhandledErrorListener, watched, dataProc);
        GetDataBuilderImpl builder = new GetDataBuilderImpl(client, stat, common.watcher, common.backgrounding, decompressed);
        return safeCall(common.internalCallback, () -> builder.forPath(path));
    }
}
