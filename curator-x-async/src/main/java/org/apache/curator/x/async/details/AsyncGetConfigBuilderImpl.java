package org.apache.curator.x.async.details;

import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.GetConfigBuilderImpl;
import org.apache.curator.x.async.AsyncEnsemblable;
import org.apache.curator.x.async.AsyncGetConfigBuilder;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.data.Stat;

import static org.apache.curator.x.async.details.BackgroundProcs.dataProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;

class AsyncGetConfigBuilderImpl implements AsyncGetConfigBuilder
{
    private final CuratorFrameworkImpl client;
    private final UnhandledErrorListener unhandledErrorListener;
    private final boolean watched;
    private Stat stat = null;

    AsyncGetConfigBuilderImpl(CuratorFrameworkImpl client, UnhandledErrorListener unhandledErrorListener, boolean watched)
    {
        this.client = client;
        this.unhandledErrorListener = unhandledErrorListener;
        this.watched = watched;
    }

    @Override
    public AsyncEnsemblable<AsyncStage<byte[]>> storingStatIn(Stat stat)
    {
        this.stat = stat;
        return this;
    }

    @Override
    public AsyncStage<byte[]> forEnsemble()
    {
        BuilderCommon<byte[]> common = new BuilderCommon<>(unhandledErrorListener, watched, dataProc);
        GetConfigBuilderImpl builder = new GetConfigBuilderImpl(client, common.backgrounding, common.watcher, stat);
        return safeCall(common.internalCallback, builder::forEnsemble);
    }
}
