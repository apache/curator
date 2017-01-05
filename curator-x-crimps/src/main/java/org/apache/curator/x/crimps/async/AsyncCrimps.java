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
                future.completeExceptionally(KeeperException.create(KeeperException.Code.get(event.getResultCode())));
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

    public CrimpedPathAndBytesable<String> name(BackgroundPathAndBytesable<String> builder)
    {
        return build(builder, nameProc);
    }

    public CrimpedPathAndBytesable<String> path(BackgroundPathAndBytesable<String> builder)
    {
        return build(builder, pathProc);
    }

    public CrimpedPathable<Void> ignored(BackgroundPathable<Void> builder)
    {
        return build(builder, ignoredProc);
    }

    public CrimpedPathable<byte[]> data(BackgroundPathable<byte[]> builder)
    {
        return build(builder, dataProc);
    }

    public CrimpedPathable<List<String>> children(BackgroundPathable<List<String>> builder)
    {
        return build(builder, childrenProc);
    }

    public CrimpedPathable<Stat> stat(BackgroundPathable<Stat> builder)
    {
        return build(builder, statProc);
    }

    public CrimpedPathable<List<ACL>> acls(BackgroundPathable<List<ACL>> builder)
    {
        return build(builder, aclProc);
    }

    public CrimpedPathAndBytesable<Stat> statBytes(BackgroundPathAndBytesable<Stat> builder)
    {
        return build(builder, statProc);
    }

    public CrimpedEnsembleable ensemble(Backgroundable<ErrorListenerEnsembleable<byte[]>> builder)
    {
        CrimpedBackgroundCallback<byte[]> callback = new CrimpedBackgroundCallback<>(dataProc);

        Ensembleable<byte[]> main;
        if ( unhandledErrorListener != null )
        {
            main = builder.inBackground(callback).withUnhandledErrorListener(unhandledErrorListener);
        }
        else
        {
            main = builder.inBackground(callback);
        }

        return new CrimpedEnsembleableImpl(main, callback);
    }

    public CrimpedEnsembleable ensemble(Backgroundable<ErrorListenerReconfigBuilderMain> builder, List<String> newMembers)
    {
        CrimpedBackgroundCallback<byte[]> callback = new CrimpedBackgroundCallback<>(dataProc);

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
        CrimpedBackgroundCallback<byte[]> callback = new CrimpedBackgroundCallback<>(dataProc);

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
        CrimpedBackgroundCallback<List<CuratorTransactionResult>> callback = new CrimpedBackgroundCallback<>(opResultsProc);
        ErrorListenerMultiTransactionMain main = builder.inBackground(callback);
        CuratorMultiTransactionMain finalBuilder = (unhandledErrorListener != null) ? main.withUnhandledErrorListener(unhandledErrorListener) : main;
        return ops -> {
            finalBuilder.forOperations(ops);
            return callback;
        };
    }

    public <T> CrimpedPathAndBytesable<T> build(BackgroundPathAndBytesable<T> builder, BackgroundProc<T> backgroundProc)
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

    public <T> CrimpedPathable<T> build(BackgroundPathable<T> builder, BackgroundProc<T> backgroundProc)
    {
        CrimpedBackgroundCallback<T> callback = new CrimpedBackgroundCallback<T>(backgroundProc);
        ErrorListenerPathable<T> localBuilder = builder.inBackground(callback);
        Pathable<T> finalLocalBuilder = (unhandledErrorListener != null) ? localBuilder.withUnhandledErrorListener(unhandledErrorListener) : localBuilder;
        return path -> {
            finalLocalBuilder.forPath(path);
            return callback;
        };
    }

    private static boolean nonEmpty(List<String> list)
    {
        return (list != null) && !list.isEmpty();
    }

}
