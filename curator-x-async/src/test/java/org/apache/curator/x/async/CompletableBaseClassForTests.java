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
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.compatibility.Timing2;
import org.testng.Assert;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

public abstract class CompletableBaseClassForTests extends BaseClassForTests
{
    protected static final Timing2 timing = new Timing2();

    protected <T, U> void complete(CompletionStage<T> stage)
    {
        complete(stage, (v, e) -> {
            if ( e != null )
            {
                Throwables.propagate(e);
            }
        });
    }

    protected <T, U> void complete(CompletionStage<T> stage, BiConsumer<? super T, Throwable> handler)
    {
        try
        {
            stage.handle((v, e) -> {
                handler.accept(v, e);
                return null;
            }).toCompletableFuture().get(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS);
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
        catch ( ExecutionException e )
        {
            if ( e.getCause() instanceof AssertionError )
            {
                throw (AssertionError)e.getCause();
            }
            Assert.fail("get() failed", e);
        }
        catch ( TimeoutException e )
        {
            Assert.fail("get() timed out");
        }
    }
}
