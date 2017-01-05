package org.apache.curator.x.crimps.async;

import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.ErrorListenerPathAndBytesable;
import org.apache.curator.framework.api.ErrorListenerPathable;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.api.Pathable;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class Crimps
{
    public static final BackgroundProc<String> nameSupplier = makeSupplier(CuratorEvent::getName);
    public static final BackgroundProc<String> pathSupplier = makeSupplier(CuratorEvent::getPath);
    public static final BackgroundProc<Void> voidSupplier = makeSupplier(e -> null);
    public static final BackgroundProc<byte[]> dataSupplier = makeSupplier(CuratorEvent::getData);
    public static final BackgroundProc<Stat> statSupplier = makeSupplier(CuratorEvent::getStat);
    public static final BackgroundProc<List<String>> childrenSupplier = makeSupplier(CuratorEvent::getChildren);
    public static final BackgroundProc<List<ACL>> aclSupplier = makeSupplier(CuratorEvent::getACLList);

    private static <T> BackgroundProc<T> makeSupplier(Function<CuratorEvent, T> proc)
    {
        return (event, future) -> {
            if ( event.getResultCode() == 0 )
            {
                future.complete(proc.apply(event));
            }
            else
            {
                future.completeExceptionally(KeeperException.create(KeeperException.Code.get(event.getResultCode())));
            }
            return null;
        };
    }

    public static CrimpedPathAndBytesable<String> nameInBackground(BackgroundPathAndBytesable<String> builder)
    {
        return build(builder, null, nameSupplier);
    }

    public static CrimpedPathAndBytesable<String> pathInBackground(BackgroundPathAndBytesable<String> builder)
    {
        return build(builder, null, pathSupplier);
    }

    public static CrimpedPathable<Void> voidInBackground(BackgroundPathable<Void> builder)
    {
        return build(builder, null, voidSupplier);
    }

    public static CrimpedPathable<byte[]> dataInBackground(BackgroundPathable<byte[]> builder)
    {
        return build(builder, null, dataSupplier);
    }

    public static CrimpedPathable<List<String>> childrenInBackground(BackgroundPathable<List<String>> builder)
    {
        return build(builder, null, childrenSupplier);
    }

    public static CrimpedPathable<Stat> statInBackground(BackgroundPathable<Stat> builder)
    {
        return build(builder, null, statSupplier);
    }

    public static CrimpedPathable<List<ACL>> aclsInBackground(BackgroundPathable<List<ACL>> builder)
    {
        return build(builder, null, aclSupplier);
    }

    public static CrimpedPathAndBytesable<Stat> statBytesInBackground(BackgroundPathAndBytesable<Stat> builder)
    {
        return build(builder, null, statSupplier);
    }

    public static <T> CrimpedPathAndBytesable<T> build(BackgroundPathAndBytesable<T> builder, UnhandledErrorListener unhandledErrorListener, BackgroundProc<T> backgroundProc)
    {
        CrimpedBackgroundCallback<T> callback = new CrimpedBackgroundCallback<T>(backgroundProc);
        ErrorListenerPathAndBytesable<T> localBuilder = builder.inBackground(callback);
        PathAndBytesable<T> finalLocalBuilder = (unhandledErrorListener != null) ? localBuilder.withUnhandledErrorListener(unhandledErrorListener) : localBuilder;
        return new CrimpedPathAndBytesable<T>()
        {
            @Override
            public CompletionStage<T> forPath(String path) throws Exception
            {
                finalLocalBuilder.forPath(path);
                return callback;
            }

            @Override
            public CompletionStage<T> forPath(String path, byte[] data) throws Exception
            {
                finalLocalBuilder.forPath(path, data);
                return callback;
            }
        };
    }

    public static <T> CrimpedPathable<T> build(BackgroundPathable<T> builder, UnhandledErrorListener unhandledErrorListener, BackgroundProc<T> backgroundProc)
    {
        CrimpedBackgroundCallback<T> callback = new CrimpedBackgroundCallback<T>(backgroundProc);
        ErrorListenerPathable<T> localBuilder = builder.inBackground(callback);
        Pathable<T> finalLocalBuilder = (unhandledErrorListener != null) ? localBuilder.withUnhandledErrorListener(unhandledErrorListener) : localBuilder;
        return path -> {
            finalLocalBuilder.forPath(path);
            return callback;
        };
    }

    private Crimps()
    {
    }
}
