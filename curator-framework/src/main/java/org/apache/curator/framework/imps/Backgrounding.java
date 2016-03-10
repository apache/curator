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

package org.apache.curator.framework.imps;

import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.KeeperException;
import java.util.concurrent.Executor;

class Backgrounding
{
    private final boolean inBackground;
    private final Object context;
    private final BackgroundCallback callback;
    private final UnhandledErrorListener errorListener;

    Backgrounding(Object context)
    {
        this.inBackground = true;
        this.context = context;
        this.callback = null;
        errorListener = null;
    }

    Backgrounding(BackgroundCallback callback)
    {
        this.inBackground = true;
        this.context = null;
        this.callback = callback;
        errorListener = null;
    }

    Backgrounding(boolean inBackground)
    {
        this.inBackground = inBackground;
        this.context = null;
        this.callback = null;
        errorListener = null;
    }

    Backgrounding(BackgroundCallback callback, Object context)
    {
        this.inBackground = true;
        this.context = context;
        this.callback = callback;
        errorListener = null;
    }

    Backgrounding(CuratorFrameworkImpl client, BackgroundCallback callback, Object context, Executor executor)
    {
        this(wrapCallback(client, callback, executor), context);
    }

    Backgrounding(CuratorFrameworkImpl client, BackgroundCallback callback, Executor executor)
    {
        this(wrapCallback(client, callback, executor));
    }

    Backgrounding(Backgrounding rhs, UnhandledErrorListener errorListener)
    {
        if ( rhs == null )
        {
            rhs = new Backgrounding();
        }
        this.inBackground = rhs.inBackground;
        this.context = rhs.context;
        this.callback = rhs.callback;
        this.errorListener = errorListener;
    }

    Backgrounding()
    {
        inBackground = false;
        context = null;
        this.callback = null;
        errorListener = null;
    }

    boolean inBackground()
    {
        return inBackground;
    }

    Object getContext()
    {
        return context;
    }

    BackgroundCallback getCallback()
    {
        return callback;
    }

    void checkError(Throwable e) throws Exception
    {
        if ( e != null )
        {
            if ( errorListener != null )
            {
                errorListener.unhandledError("n/a", e);
            }
            else if ( e instanceof Exception )
            {
                throw (Exception)e;
            }
            else
            {
                Throwables.propagate(e);
            }
        }
    }

    private static BackgroundCallback wrapCallback(final CuratorFrameworkImpl client, final BackgroundCallback callback, final Executor executor)
    {
        return new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework dummy, final CuratorEvent event) throws Exception
            {
                executor.execute
                    (
                        new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                try
                                {
                                    callback.processResult(client, event);
                                }
                                catch ( Exception e )
                                {
                                    ThreadUtils.checkInterrupted(e);
                                    if ( e instanceof KeeperException )
                                    {
                                        client.validateConnection(client.codeToState(((KeeperException)e).code()));
                                    }
                                    client.logError("Background operation result handling threw exception", e);
                                }
                            }
                        }
                    );
            }
        };
    }
}
