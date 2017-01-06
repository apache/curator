/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.curator.x.crimps.async;

import org.apache.curator.framework.api.*;
import org.apache.curator.framework.api.transaction.CuratorMultiTransactionMain;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class AsyncCrimps
{
    public static final BackgroundProc<String> nameProc = makeProc(CuratorEvent::getName);
    public static final BackgroundProc<String> pathProc = makeProc(CuratorEvent::getPath);
    public static final BackgroundProc<Void> ignoredProc = makeProc(e -> null);
    public static final BackgroundProc<byte[]> dataProc = makeProc(CuratorEvent::getData);
    public static final BackgroundProc<Stat> statProc = makeProc(CuratorEvent::getStat);
    public static final BackgroundProc<Stat> safeStatProc = (event, future) -> {
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
    public static final BackgroundProc<List<String>> childrenProc = makeProc(CuratorEvent::getChildren);
    public static final BackgroundProc<List<ACL>> aclProc = makeProc(CuratorEvent::getACLList);
    public static final BackgroundProc<List<CuratorTransactionResult>> opResultsProc = makeProc(CuratorEvent::getOpResults);

    private final UnhandledErrorListener unhandledErrorListener;

    public static <T> BackgroundProc<T> makeProc(Function<CuratorEvent, T> proc)
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

    public AsyncCrimps(UnhandledErrorListener unhandledErrorListener)
    {
        this.unhandledErrorListener = unhandledErrorListener;
    }

    public AsyncCrimps withUnhandledErrorListener(UnhandledErrorListener unhandledErrorListener)
    {
        return new AsyncCrimps(unhandledErrorListener);
    }

    public CrimpledPathAndBytesable<CompletionStage<String>> name(BackgroundPathAndBytesable<String> builder)
    {
        return build(builder, nameProc);
    }

    public CrimpledPathAndBytesable<CompletionStage<String>> path(BackgroundPathAndBytesable<String> builder)
    {
        return build(builder, pathProc);
    }

    public CrimpedPathable<CompletionStage<Void>> ignored(BackgroundPathable<Void> builder)
    {
        return build(builder, ignoredProc);
    }

    public CrimpedPathable<CompletionStage<byte[]>> data(BackgroundPathable<byte[]> builder)
    {
        return build(builder, dataProc);
    }

    public CrimpedPathable<Crimped<byte[]>> dataWatched(Watchable<BackgroundPathable<byte[]>> builder)
    {
        return build(builder, dataProc);
    }

    public CrimpedPathable<CompletionStage<List<String>>> children(BackgroundPathable<List<String>> builder)
    {
        return build(builder, childrenProc);
    }

    public CrimpedPathable<Crimped<List<String>>> childrenWatched(Watchable<BackgroundPathable<List<String>>> builder)
    {
        return build(builder, childrenProc);
    }

    public CrimpedPathable<CompletionStage<Stat>> stat(BackgroundPathable<Stat> builder)
    {
        return build(builder, safeStatProc);
    }

    public CrimpedPathable<Crimped<Stat>> statWatched(Watchable<BackgroundPathable<Stat>> builder)
    {
        return build(builder, safeStatProc);
    }

    public CrimpedPathable<CompletionStage<List<ACL>>> acls(BackgroundPathable<List<ACL>> builder)
    {
        return build(builder, aclProc);
    }

    public CrimpledPathAndBytesable<CompletionStage<Stat>> statBytes(BackgroundPathAndBytesable<Stat> builder)
    {
        return build(builder, statProc);
    }

    public CrimpedWatchedEnsembleable ensembleWatched(Watchable<BackgroundEnsembleable<byte[]>> builder)
    {
        CrimpedWatcher crimpedWatcher = new CrimpedWatcher();
        CrimpedBackgroundCallback<byte[]> callback = new CrimpedBackgroundCallback<>(dataProc, crimpedWatcher);
        BackgroundEnsembleable<byte[]> localBuilder = builder.usingWatcher(crimpedWatcher);
        return new CrimpedWatchedEnsembleableImpl(toFinalBuilder(callback, localBuilder), callback);
    }

    public CrimpedEnsembleable ensemble(Backgroundable<ErrorListenerEnsembleable<byte[]>> builder)
    {
        CrimpedBackgroundCallback<byte[]> callback = new CrimpedBackgroundCallback<>(dataProc, null);
        return new CrimpedEnsembleableImpl(toFinalBuilder(callback, builder), callback);
    }

    public CrimpedEnsembleable ensemble(Backgroundable<ErrorListenerReconfigBuilderMain> builder, List<String> newMembers)
    {
        CrimpedBackgroundCallback<byte[]> callback = new CrimpedBackgroundCallback<>(dataProc, null);

        ReconfigBuilderMain main;
        if ( unhandledErrorListener != null )
        {
            main = builder.inBackground(callback).withUnhandledErrorListener(unhandledErrorListener);
        }
        else
        {
            main = builder.inBackground(callback);
        }

        return new CrimpedEnsembleableImpl((Statable<ConfigureEnsembleable>)main.withNewMembers(newMembers), callback);
    }

    public CrimpedEnsembleable ensemble(Backgroundable<ErrorListenerReconfigBuilderMain> builder, List<String> joining, List<String> leaving)
    {
        CrimpedBackgroundCallback<byte[]> callback = new CrimpedBackgroundCallback<>(dataProc, null);

        ReconfigBuilderMain main;
        if ( unhandledErrorListener != null )
        {
            main = builder.inBackground(callback).withUnhandledErrorListener(unhandledErrorListener);
        }
        else
        {
            main = builder.inBackground(callback);
        }

        Statable<ConfigureEnsembleable> configBuilder;
        if ( nonEmpty(joining) && nonEmpty(leaving) )
        {
            configBuilder = main.joining(joining).leaving(leaving);
        }
        else if ( nonEmpty(joining) )
        {
            configBuilder = main.joining(joining);
        }
        else if ( nonEmpty(leaving) )
        {
            configBuilder = main.leaving(leaving);
        }
        else
        {
            throw new IllegalArgumentException("leaving and joining cannot both be empty");
        }

        return new CrimpedEnsembleableImpl(configBuilder, callback);
    }

    public CrimpedMultiTransaction opResults(Backgroundable<ErrorListenerMultiTransactionMain> builder)
    {
        CrimpedBackgroundCallback<List<CuratorTransactionResult>> callback = new CrimpedBackgroundCallback<>(opResultsProc, null);
        ErrorListenerMultiTransactionMain main = builder.inBackground(callback);
        CuratorMultiTransactionMain finalBuilder = (unhandledErrorListener != null) ? main.withUnhandledErrorListener(unhandledErrorListener) : main;
        return ops -> {
            try
            {
                finalBuilder.forOperations(ops);
            }
            catch ( Exception e )
            {
                callback.completeExceptionally(e);
            }
            return callback;
        };
    }

    public <T> CrimpledPathAndBytesable<CompletionStage<T>> build(BackgroundPathAndBytesable<T> builder, BackgroundProc<T> backgroundProc)
    {
        CrimpedBackgroundCallback<T> callback = new CrimpedBackgroundCallback<T>(backgroundProc, null);
        ErrorListenerPathAndBytesable<T> localBuilder = builder.inBackground(callback);
        PathAndBytesable<T> finalLocalBuilder = (unhandledErrorListener != null) ? localBuilder.withUnhandledErrorListener(unhandledErrorListener) : localBuilder;
        return new CrimpledPathAndBytesableImpl<>(finalLocalBuilder, callback, null);
    }

    public <T> CrimpedPathable<Crimped<T>> build(Watchable<BackgroundPathable<T>> builder, BackgroundProc<T> backgroundProc)
    {
        CrimpedWatcher crimpedWatcher = new CrimpedWatcher();
        CrimpedBackgroundCallback<T> callback = new CrimpedBackgroundCallback<>(backgroundProc, crimpedWatcher);
        Pathable<T> finalLocalBuilder = toFinalBuilder(callback, builder.usingWatcher(crimpedWatcher));
        return new CrimpledPathAndBytesableImpl<T, Crimped<T>>(finalLocalBuilder, callback, crimpedWatcher);
    }

    public <T> CrimpedPathable<CompletionStage<T>> build(BackgroundPathable<T> builder, BackgroundProc<T> backgroundProc)
    {
        CrimpedBackgroundCallback<T> callback = new CrimpedBackgroundCallback<T>(backgroundProc, null);
        Pathable<T> finalLocalBuilder = toFinalBuilder(callback, builder);
        return new CrimpledPathAndBytesableImpl<>(finalLocalBuilder, callback, null);
    }

    private Ensembleable<byte[]> toFinalBuilder(CrimpedBackgroundCallback<byte[]> callback, Backgroundable<ErrorListenerEnsembleable<byte[]>> builder)
    {
        ErrorListenerEnsembleable<byte[]> localBuilder = builder.inBackground(callback);
        return (unhandledErrorListener != null) ? localBuilder.withUnhandledErrorListener(unhandledErrorListener) : localBuilder;
    }

    private <T> Pathable<T> toFinalBuilder(CrimpedBackgroundCallback<T> callback, BackgroundPathable<T> builder)
    {
        ErrorListenerPathable<T> localBuilder = builder.inBackground(callback);
        return (unhandledErrorListener != null) ? localBuilder.withUnhandledErrorListener(unhandledErrorListener) : localBuilder;
    }

    private static boolean nonEmpty(List<String> list)
    {
        return (list != null) && !list.isEmpty();
    }
}
