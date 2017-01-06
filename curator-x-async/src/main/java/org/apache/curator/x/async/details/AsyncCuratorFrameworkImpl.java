package org.apache.curator.x.async.details;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.CuratorMultiTransactionImpl;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncCuratorFrameworkDsl;
import org.apache.curator.x.async.AsyncDeleteBuilder;
import org.apache.curator.x.async.AsyncExistsBuilder;
import org.apache.curator.x.async.AsyncGetChildrenBuilder;
import org.apache.curator.x.async.AsyncGetDataBuilder;
import org.apache.curator.x.async.AsyncMultiTransaction;
import org.apache.curator.x.async.AsyncSetDataBuilder;
import org.apache.curator.x.async.WatchedAsyncCuratorFramework;
import java.util.List;

import static org.apache.curator.x.async.details.BackgroundProcs.opResultsProc;
import static org.apache.curator.x.async.details.BackgroundProcs.safeCall;

public class AsyncCuratorFrameworkImpl implements AsyncCuratorFramework
{
    private final CuratorFrameworkImpl client;
    private final UnhandledErrorListener unhandledErrorListener;
    private final boolean watched;

    public AsyncCuratorFrameworkImpl(CuratorFrameworkImpl client, UnhandledErrorListener unhandledErrorListener, boolean watched)
    {
        this.client = client;
        this.unhandledErrorListener = unhandledErrorListener;
        this.watched = watched;
    }

    @Override
    public CreateBuilder create()
    {
        return null;
    }

    @Override
    public AsyncDeleteBuilder delete()
    {
        return new AsyncDeleteBuilderImpl(client, unhandledErrorListener);
    }

    @Override
    public AsyncSetDataBuilder setData()
    {
        return new AsyncSetDataBuilderImpl(client, unhandledErrorListener);
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
    public AsyncMultiTransaction transaction()
    {
        return operations -> {
            BuilderCommon<List<CuratorTransactionResult>> common = new BuilderCommon<>(unhandledErrorListener, false, opResultsProc);
            CuratorMultiTransactionImpl builder = new CuratorMultiTransactionImpl(client, common.backgrounding);
            return safeCall(common.internalCallback, () -> builder.forOperations(operations));
        };
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

    @Override
    public CuratorFramework getCuratorFramework()
    {
        return client;
    }

    @Override
    public WatchedAsyncCuratorFramework watched()
    {
        return new AsyncCuratorFrameworkImpl(client, unhandledErrorListener, true);
    }

    @Override
    public AsyncCuratorFrameworkDsl withUnhandledErrorListener(UnhandledErrorListener listener)
    {
        return new AsyncCuratorFrameworkImpl(client, listener, watched);
    }

    @Override
    public TransactionOp transactionOp()
    {
        return client.transactionOp();
    }

    @Override
    public AsyncExistsBuilder checkExists()
    {
        return new AsyncExistsBuilderImpl(client, unhandledErrorListener, watched);
    }

    @Override
    public AsyncGetDataBuilder getData()
    {
        return new AsyncGetDataBuilderImpl(client, unhandledErrorListener, watched);
    }

    @Override
    public AsyncGetChildrenBuilder getChildren()
    {
        return new AsyncGetChildrenBuilderImpl(client, unhandledErrorListener, watched);
    }

    @Override
    public GetConfigBuilder getConfig()
    {
        return null;
    }
}
