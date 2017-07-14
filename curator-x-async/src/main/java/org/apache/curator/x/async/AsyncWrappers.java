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
package org.apache.curator.x.async;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.async.api.ExistsOption;
import org.apache.zookeeper.KeeperException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     Utility for adding asynchronous behavior
 * </p>
 *
 * <p>
 *     E.g. locks:
 * <code><pre>
 *     InterProcessMutex mutex = new InterProcessMutex(...) // or any InterProcessLock
 *     AsyncWrappers.lockAsync(mutex, executor).thenAccept(dummy -> {
 *         try
 *         {
 *             // do work while holding the lock
 *         }
 *         finally
 *         {
 *             AsyncWrappers.release(mutex);
 *         }
 *     }).exceptionally(e -> {
 *         if ( e instanceOf TimeoutException ) {
 *             // timed out trying to acquire the lock
 *         }
 *         // handle the error
 *         return null;
 *     });
 * </pre></code>
 * </p>
 *
 * <p>
 *     E.g. EnsureContainers
 * <code><pre>
 *     AsyncWrappers.(client, path, executor).thenAccept(dummy -> {
 *         // execute after ensuring containers
 *     });
 * </pre></code>
 * </p>
 */
public class AsyncWrappers
{
    /**
     * <p>
     * Return the children of the given path (keyed by the full path) and the data for each node.
     * IMPORTANT: this results in a ZooKeeper query
     * for each child node returned. i.e. if the initial children() call returns
     * 10 nodes an additional 10 ZooKeeper queries are made to get the data.
     * </p>
     *
     * <p>
     * Note: if the any of the nodes in the path do not exist yet, {@link org.apache.zookeeper.KeeperException.NoNodeException}
     * is <strong>NOT</strong> set. Instead the stage is completed with an empty map.
     * </p>
     *
     * @return CompletionStage
     */
    public static CompletionStage<Map<String, byte[]>> childrenWithData(AsyncCuratorFramework client, String path)
    {
        return childrenWithData(client, path, false);
    }

    /**
     * <p>
     * Return the children of the given path (keyed by the full path) and the data for each node.
     * IMPORTANT: this results in a ZooKeeper query
     * for each child node returned. i.e. if the initial children() call returns
     * 10 nodes an additional 10 ZooKeeper queries are made to get the data.
     * </p>
     *
     * <p>
     * Note: if the any of the nodes in the path do not exist yet, {@link org.apache.zookeeper.KeeperException.NoNodeException}
     * is <strong>NOT</strong> set. Instead the stage is completed with an empty map.
     * </p>
     *
     * @param isCompressed pass true if data is compressed
     * @return CompletionStage
     */
    public static CompletionStage<Map<String, byte[]>> childrenWithData(AsyncCuratorFramework client, String path, boolean isCompressed)
    {
        CompletableFuture<Map<String, byte[]>> future = new CompletableFuture<>();
        client.getChildren().forPath(path).handle((children, e) -> {
            if ( e != null )
            {
                if ( Throwables.getRootCause(e) instanceof KeeperException.NoNodeException )
                {
                    future.complete(Maps.newHashMap());
                }
                else
                {
                    future.completeExceptionally(e);
                }
            }
            else
            {
                completeChildren(client, future, path, children, isCompressed);
            }
            return null;
        });
        return future;
    }

    /**
     * Asynchronously call {@link org.apache.curator.framework.CuratorFramework#createContainers(String)} using
     * the given executor
     *
     * @param client client
     * @param path path to ensure
     * @return stage
     */
    public static CompletionStage<Void> asyncEnsureContainers(AsyncCuratorFramework client, String path)
    {
        String localPath = ZKPaths.makePath(path, "foo");
        Set<ExistsOption> options = Collections.singleton(ExistsOption.createParentsAsContainers);
        return client
            .checkExists()
            .withOptions(options)
            .forPath(localPath)
            .thenApply(__ -> null)
            ;
    }

    /**
     * Set as the completion stage's exception when trying to acquire a lock
     * times out
     */
    public static class TimeoutException extends RuntimeException
    {
    }

    /**
     * Attempt to acquire the given lock asynchronously using the given timeout and executor. If the lock
     * is not acquired within the timeout stage is completedExceptionally with {@link AsyncWrappers.TimeoutException}
     *
     * @param lock a lock implementation (e.g. {@link org.apache.curator.framework.recipes.locks.InterProcessMutex},
     * {@link org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2}, etc.)
     * @param timeout max timeout to acquire lock
     * @param unit time unit of timeout
     * @param executor executor to use to asynchronously acquire
     * @return stage
     */
    public static CompletionStage<Void> lockAsync(InterProcessLock lock, long timeout, TimeUnit unit, Executor executor)
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if ( executor == null )
        {
            CompletableFuture.runAsync(() -> lock(future, lock, timeout, unit));
        }
        else
        {
            CompletableFuture.runAsync(() -> lock(future, lock, timeout, unit), executor);
        }
        return future;
    }

    /**
     * Attempt to acquire the given lock asynchronously using the given timeout and executor. The stage
     * is completed with a Boolean that indicates whether or not the lock was acquired.
     *
     * @param lock a lock implementation (e.g. {@link org.apache.curator.framework.recipes.locks.InterProcessMutex},
     * {@link org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2}, etc.)
     * @param timeout max timeout to acquire lock
     * @param unit time unit of timeout
     * @param executor executor to use to asynchronously acquire
     * @return stage
     */
    public static CompletionStage<Boolean> lockAsyncIf(InterProcessLock lock, long timeout, TimeUnit unit, Executor executor)
    {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        if ( executor == null )
        {
            CompletableFuture.runAsync(() -> lockIf(future, lock, timeout, unit));
        }
        else
        {
            CompletableFuture.runAsync(() -> lockIf(future, lock, timeout, unit), executor);
        }
        return future;
    }

    /**
     * Attempt to acquire the given lock asynchronously using the given executor and without a timeout.
     *
     * @param lock a lock implementation (e.g. {@link org.apache.curator.framework.recipes.locks.InterProcessMutex},
     * {@link org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2}, etc.)
     * @param executor executor to use to asynchronously acquire
     * @return stage
     */
    public static CompletionStage<Void> lockAsync(InterProcessLock lock, Executor executor)
    {
        return lockAsync(lock, 0, null, executor);
    }

    /**
     * Attempt to acquire the given lock asynchronously using the given timeout using the {@link java.util.concurrent.ForkJoinPool#commonPool()}.
     * If the lock is not acquired within the timeout stage is completedExceptionally with {@link AsyncWrappers.TimeoutException}
     *
     * @param lock a lock implementation (e.g. {@link org.apache.curator.framework.recipes.locks.InterProcessMutex},
     * {@link org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2}, etc.)
     * @param timeout max timeout to acquire lock
     * @param unit time unit of timeout
     * @return stage
     */
    public static CompletionStage<Void> lockAsync(InterProcessLock lock, long timeout, TimeUnit unit)
    {
        return lockAsync(lock, timeout, unit, null);
    }

    /**
     * Attempt to acquire the given lock asynchronously using the given timeout using the {@link java.util.concurrent.ForkJoinPool#commonPool()}.
     * The stage is completed with a Boolean that indicates whether or not the lock was acquired.
     *
     * @param lock a lock implementation (e.g. {@link org.apache.curator.framework.recipes.locks.InterProcessMutex},
     * {@link org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2}, etc.)
     * @param timeout max timeout to acquire lock
     * @param unit time unit of timeout
     * @return stage
     */
    public static CompletionStage<Boolean> lockAsyncIf(InterProcessLock lock, long timeout, TimeUnit unit)
    {
        return lockAsyncIf(lock, timeout, unit, null);
    }

    /**
     * Attempt to acquire the given lock asynchronously without timeout using the {@link java.util.concurrent.ForkJoinPool#commonPool()}.
     *
     * @param lock a lock implementation (e.g. {@link org.apache.curator.framework.recipes.locks.InterProcessMutex},
     * {@link org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2}, etc.)
     * @return stage
     */
    public static CompletionStage<Void> lockAsync(InterProcessLock lock)
    {
        return lockAsync(lock, 0, null, null);
    }

    /**
     * Release the lock and wrap any exception in <code>RuntimeException</code>
     *
     * @param lock lock to release
     */
    public static void release(InterProcessLock lock)
    {
        release(lock, true);
    }

    /**
     * Release the lock and wrap any exception in <code>RuntimeException</code>
     *
     * @param lock lock to release
     * @param ignoreNoLockExceptions if true {@link java.lang.IllegalStateException} is ignored
     */
    public static void release(InterProcessLock lock, boolean ignoreNoLockExceptions)
    {
        try
        {
            lock.release();
        }
        catch ( IllegalStateException e )
        {
            if ( !ignoreNoLockExceptions )
            {
                throw new RuntimeException(e);
            }
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RuntimeException(e);
        }
    }

    private static void lockIf(CompletableFuture<Boolean> future, InterProcessLock lock, long timeout, TimeUnit unit)
    {
        try
        {
            future.complete(lock.acquire(timeout, unit));
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            future.completeExceptionally(e);
        }
    }

    private static void lock(CompletableFuture<Void> future, InterProcessLock lock, long timeout, TimeUnit unit)
    {
        try
        {
            if ( unit != null )
            {
                if ( lock.acquire(timeout, unit) )
                {
                    future.complete(null);
                }
                else
                {
                    future.completeExceptionally(new TimeoutException());
                }
            }
            else
            {
                lock.acquire();
                future.complete(null);
            }
        }
        catch ( Throwable e )
        {
            ThreadUtils.checkInterrupted(e);
            future.completeExceptionally(e);
        }
    }

    private static void completeChildren(AsyncCuratorFramework client, CompletableFuture<Map<String, byte[]>> future, String parentPath, List<String> children, boolean isCompressed)
    {
        Map<String, byte[]> nodes = Maps.newHashMap();
        if ( children.size() == 0 )
        {
            future.complete(nodes);
            return;
        }

        children.forEach(node -> {
            String path = ZKPaths.makePath(parentPath, node);
            AsyncStage<byte[]> stage = isCompressed ? client.getData().decompressed().forPath(path) : client.getData().forPath(path);
            stage.handle((data, e) -> {
                if ( e != null )
                {
                    future.completeExceptionally(e);
                }
                else
                {
                    nodes.put(path, data);
                    if ( nodes.size() == children.size() )
                    {
                        future.complete(nodes);
                    }
                }
                return null;
            });
        });
    }

    private AsyncWrappers()
    {
    }
}
