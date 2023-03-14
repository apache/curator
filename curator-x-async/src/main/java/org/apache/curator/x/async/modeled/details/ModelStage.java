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

package org.apache.curator.x.async.modeled.details;

import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.WatchedEvent;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

class ModelStage<T> extends CompletableFuture<T> implements AsyncStage<T>
{
    private final CompletionStage<WatchedEvent> event;

    static <U> ModelStage<U> make()
    {
        return new ModelStage<>(null);
    }

    static <U> ModelStage<U> make(CompletionStage<WatchedEvent> event)
    {
        return new ModelStage<>(event);
    }

    static <U> ModelStage<U> completed(U value)
    {
        ModelStage<U> stage = new ModelStage<>(null);
        stage.complete(value);
        return stage;
    }

    static <U> ModelStage<U> exceptionally(Exception e)
    {
        ModelStage<U> stage = new ModelStage<>(null);
        stage.completeExceptionally(e);
        return stage;
    }

    static <U> ModelStage<U> async(Executor executor)
    {
        return new AsyncModelStage<>(executor);
    }

    static <U> ModelStage<U> asyncCompleted(U value, Executor executor)
    {
        ModelStage<U> stage = new AsyncModelStage<>(executor);
        stage.complete(value);
        return stage;
    }

    static <U> ModelStage<U> asyncExceptionally(Exception e, Executor executor)
    {
        ModelStage<U> stage = new AsyncModelStage<>(executor);
        stage.completeExceptionally(e);
        return stage;
    }

    @Override
    public CompletionStage<WatchedEvent> event()
    {
        return event;
    }

    private ModelStage(CompletionStage<WatchedEvent> event)
    {
        this.event = event;
    }

    private static class AsyncModelStage<U> extends ModelStage<U>
    {
        private final Executor executor;

        public AsyncModelStage(Executor executor)
        {
            super(null);
            this.executor = executor;
        }

        @Override
        public <U1> CompletableFuture<U1> thenApplyAsync(Function<? super U, ? extends U1> fn)
        {
            return super.thenApplyAsync(fn, executor);
        }

        @Override
        public CompletableFuture<Void> thenAcceptAsync(Consumer<? super U> action)
        {
            return super.thenAcceptAsync(action, executor);
        }

        @Override
        public CompletableFuture<Void> thenRunAsync(Runnable action)
        {
            return super.thenRunAsync(action, executor);
        }

        @Override
        public <U1, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U1> other, BiFunction<? super U, ? super U1, ? extends V> fn)
        {
            return super.thenCombineAsync(other, fn, executor);
        }

        @Override
        public <U1> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U1> other, BiConsumer<? super U, ? super U1> action)
        {
            return super.thenAcceptBothAsync(other, action, executor);
        }

        @Override
        public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action)
        {
            return super.runAfterBothAsync(other, action, executor);
        }

        @Override
        public <U1> CompletableFuture<U1> applyToEitherAsync(CompletionStage<? extends U> other, Function<? super U, U1> fn)
        {
            return super.applyToEitherAsync(other, fn, executor);
        }

        @Override
        public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends U> other, Consumer<? super U> action)
        {
            return super.acceptEitherAsync(other, action, executor);
        }

        @Override
        public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action)
        {
            return super.runAfterEitherAsync(other, action, executor);
        }

        @Override
        public <U1> CompletableFuture<U1> thenComposeAsync(Function<? super U, ? extends CompletionStage<U1>> fn)
        {
            return super.thenComposeAsync(fn, executor);
        }

        @Override
        public CompletableFuture<U> whenCompleteAsync(BiConsumer<? super U, ? super Throwable> action)
        {
            return super.whenCompleteAsync(action, executor);
        }

        @Override
        public <U1> CompletableFuture<U1> handleAsync(BiFunction<? super U, Throwable, ? extends U1> fn)
        {
            return super.handleAsync(fn, executor);
        }
    }
}
