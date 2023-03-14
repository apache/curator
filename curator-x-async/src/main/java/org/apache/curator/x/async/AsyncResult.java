/*
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

import org.apache.curator.x.async.details.AsyncResultImpl;
import org.apache.zookeeper.KeeperException;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * <p>
 *     Utility that combines the value, the ZooKeeper result code and the exception in one object
 *     allowing you to not worry about exceptional completions. i.e. the {@link java.util.concurrent.CompletionStage}
 *     returned by {@link org.apache.curator.x.async.AsyncResult#of(AsyncStage)} always completes successfully with an
 *     {@link org.apache.curator.x.async.AsyncResult} object.
 * </p>
 *
 * <p>
 *     All three possible results from a ZooKeeper method are encapsulated in this object. If the ZooKeeper
 *     method succeeds, the internal value will be set. If there was a standard ZooKeeper error code
 *     ({@link org.apache.zookeeper.KeeperException.Code#NODEEXISTS}, etc.), that code is set and the
 *     value is null. If there was a general exception, that exception is set, the value will be null
 *     and the code will be {@link org.apache.zookeeper.KeeperException.Code#SYSTEMERROR}.
 * </p>
 * @param <T> value type
 */
public interface AsyncResult<T>
{
    /**
     * Return a new stage that wraps an async stage into a result-style completion stage. The returned
     * CompletionStage will always complete successfully.
     *
     * @param stage the stage to wrap
     * @param <T> value type
     * @return completion stage that resolves to a result
     */
    static <T> CompletionStage<AsyncResult<T>> of(AsyncStage<T> stage)
    {
        return stage.handle((value, ex) -> {
            if ( ex != null )
            {
                if ( ex instanceof KeeperException )
                {
                    return new AsyncResultImpl<T>(((KeeperException)ex).code());
                }
                return new AsyncResultImpl<T>(ex);
            }
            return new AsyncResultImpl<T>(value);
        });
    }

    /**
     * Returns the raw result of the ZooKeeper method or <code>null</code>
     *
     * @return result or <code>null</code>
     */
    T getRawValue();

    /**
     * An optional wrapper around the ZooKeeper method result
     *
     * @return wrapped result
     */
    Optional<T> getValue();

    /**
     * Return the ZooKeeper result code. If the method was successful,
     * {@link org.apache.zookeeper.KeeperException.Code#OK} is returned. If there was a general
     * exception {@link org.apache.zookeeper.KeeperException.Code#SYSTEMERROR} is returned.
     *
     * @return result code
     */
    KeeperException.Code getCode();

    /**
     * Return any general exception or <code>null</code>
     *
     * @return exception or <code>null</code>
     */
    Throwable getRawException();

    /**
     * An optional wrapper around any general exception
     *
     * @return wrapped exception
     */
    Optional<Throwable> getException();

    /**
     * If there was a general exception (but <strong>not</strong> a {@link org.apache.zookeeper.KeeperException})
     * a {@link java.lang.RuntimeException} is thrown that wraps the exception. Otherwise, the method returns
     * without any action being performed.
     */
    void checkException();

    /**
     * If there was a general exception or a {@link org.apache.zookeeper.KeeperException}
     * a {@link java.lang.RuntimeException} is thrown that wraps the exception. Otherwise, the method returns
     * without any action being performed.
     */
    void checkError();
}
