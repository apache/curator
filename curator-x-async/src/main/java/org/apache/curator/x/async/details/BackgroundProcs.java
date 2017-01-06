package org.apache.curator.x.async.details;

import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;

class BackgroundProcs
{
    static final BackgroundProc<String> nameProc = makeProc(CuratorEvent::getName);
    static final BackgroundProc<String> pathProc = makeProc(CuratorEvent::getPath);
    static final BackgroundProc<Void> ignoredProc = makeProc(e -> null);
    static final BackgroundProc<byte[]> dataProc = makeProc(CuratorEvent::getData);
    static final BackgroundProc<Stat> statProc = makeProc(CuratorEvent::getStat);
    static final BackgroundProc<Stat> safeStatProc = (event, future) -> {
        if ( (event.getResultCode() == 0) || (event.getResultCode() == KeeperException.Code.NONODE.intValue()) )
        {
            future.complete(event.getStat());
        }
        else
        {
            future.completeExceptionally(KeeperException.create(KeeperException.Code.get(event.getResultCode()), event.getPath()));
        }
        return null;
    };
    static final BackgroundProc<List<String>> childrenProc = makeProc(CuratorEvent::getChildren);
    static final BackgroundProc<List<ACL>> aclProc = makeProc(CuratorEvent::getACLList);
    static final BackgroundProc<List<CuratorTransactionResult>> opResultsProc = makeProc(CuratorEvent::getOpResults);

    static <T> BackgroundProc<T> makeProc(Function<CuratorEvent, T> proc)
    {
        return (event, future) -> {
            if ( event.getResultCode() == 0 )
            {
                future.complete(proc.apply(event));
            }
            else
            {
                future.completeExceptionally(KeeperException.create(KeeperException.Code.get(event.getResultCode()), event.getPath()));
            }
            return null;
        };
    }

    static <T> InternalCallback<T> safeCall(InternalCallback<T> callback, Callable<?> proc)
    {
        try
        {
            proc.call();
        }
        catch ( Exception e )
        {
            callback.toCompletableFuture().completeExceptionally(e);
        }
        return callback;
    }

    BackgroundProcs()
    {
    }
}
