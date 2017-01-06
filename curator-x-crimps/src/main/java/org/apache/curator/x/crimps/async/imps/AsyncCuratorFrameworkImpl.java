package org.apache.curator.x.crimps.async.imps;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.x.crimps.async.AsyncCuratorFramework;
import org.apache.curator.x.crimps.async.AsyncCuratorFrameworkDsl;
import org.apache.curator.x.crimps.async.AsyncDeleteBuilder;
import org.apache.curator.x.crimps.async.AsyncGetChildrenBuilder;
import org.apache.curator.x.crimps.async.AsyncMultiTransaction;
import org.apache.curator.x.crimps.async.AsyncSetDataBuilder;
import org.apache.curator.x.crimps.async.WatchedAsyncCuratorFramework;
import org.apache.curator.x.crimps.async.details.AsyncCrimps;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class AsyncCuratorFrameworkImpl implements AsyncCuratorFramework
{
    private final CuratorFramework client;
    private final AsyncCrimps async;
    private final UnhandledErrorListener unhandledErrorListener;

    public AsyncCuratorFrameworkImpl(CuratorFramework client, AsyncCrimps async, UnhandledErrorListener unhandledErrorListener)
    {
        this.client = client;
        this.async = async;
        this.unhandledErrorListener = unhandledErrorListener;
    }

    @Override
    public AsyncCuratorFrameworkDsl withUnhandledErrorListener(UnhandledErrorListener listener)
    {
        return new AsyncCuratorFrameworkImpl(client, async, listener);
    }

    @Override
    public CuratorFramework getCuratorFramework()
    {
        return client;
    }

    @Override
    public WatchedAsyncCuratorFramework watched()
    {
        return new WatchedAsyncCuratorFrameworkImpl(client, async, unhandledErrorListener);
    }

    @Override
    public CreateBuilder create()
    {
        return null;
    }

    @Override
    public AsyncDeleteBuilder delete()
    {
        return new AsyncDeleteBuilderImpl(client, async, unhandledErrorListener);
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
    public AsyncSetDataBuilder setData()
    {
        return new AsyncSetDataBuilderImpl(client, async, unhandledErrorListener);
    }

    @Override
    public AsyncGetChildrenBuilder<CompletionStage<List<String>>> getChildren()
    {
        return new AsyncGetChildrenBuilderImpl<CompletionStage<List<String>>>(client, unhandledErrorListener)
        {
            @Override
            protected CompletionStage<List<String>> finish(BackgroundPathable<List<String>> builder, String path)
            {
                return async.children(builder).forPath(path);
            }
        };
    }

    @Override
    public GetACLBuilder getACL()
    {
        return null;
    }

    @Override
    public SetACLBuilder setACL()
    {
        return null;
    }

    @Override
    public ReconfigBuilder reconfig()
    {
        return null;
    }

    @Override
    public GetConfigBuilder getConfig()
    {
        return null;
    }

    @Override
    public AsyncMultiTransaction transaction()
    {
        return operations -> async.opResults(client.transaction()).forOperations(operations);
    }

    @Override
    public TransactionOp transactionOp()
    {
        return null;
    }

    @Override
    public SyncBuilder sync()
    {
        return null;
    }

    @Override
    public RemoveWatchesBuilder watches()
    {
        return null;
    }

    public UnhandledErrorListener getUnhandledErrorListener()
    {
        return unhandledErrorListener;
    }

}
