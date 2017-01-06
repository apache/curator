package org.apache.curator.x.async.details;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetACLBuilder;
import org.apache.curator.framework.api.ReconfigBuilder;
import org.apache.curator.framework.api.RemoveWatchesBuilder;
import org.apache.curator.framework.api.SetACLBuilder;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.imps.CuratorMultiTransactionImpl;
import org.apache.curator.framework.imps.SyncBuilderImpl;
import org.apache.curator.x.async.*;
import java.util.List;

import static org.apache.curator.x.async.details.BackgroundProcs.*;

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
    public AsyncCreateBuilder create()
    {
        return new AsyncCreateBuilderImpl(client, unhandledErrorListener);
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
    public AsyncReconfigBuilder reconfig()
    {
        return new AsyncReconfigBuilderImpl(client, unhandledErrorListener);
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
    public AsyncSyncBuilder sync()
    {
        return path -> {
            BuilderCommon<Void> common = new BuilderCommon<>(unhandledErrorListener, false, ignoredProc);
            SyncBuilderImpl builder = new SyncBuilderImpl(client, common.backgrounding);
            return safeCall(common.internalCallback, () -> builder.forPath(path));
        };
    }

    @Override
    public AsyncRemoveWatchesBuilder watches()
    {
        return new AsyncRemoveWatchesBuilderImpl(client, unhandledErrorListener);
    }

    @Override
    public CuratorFramework unwrap()
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
    public AsyncTransactionOp transactionOp()
    {
        return new AsyncTransactionOpImpl(client);
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
    public AsyncGetConfigBuilder getConfig()
    {
        return new AsyncGetConfigBuilderImpl(client, unhandledErrorListener, watched);
    }
}
