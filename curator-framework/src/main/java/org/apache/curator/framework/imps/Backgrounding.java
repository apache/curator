/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package org.apache.curator.framework.imps;

import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.CuratorFramework;
import java.util.concurrent.Executor;

class Backgrounding
{
    private final boolean               inBackground;
    private final Object                context;
    private final BackgroundCallback    callback;

    Backgrounding(Object context)
    {
        this.inBackground = true;
        this.context = context;
        this.callback = null;
    }

    Backgrounding(BackgroundCallback callback)
    {
        this.inBackground = true;
        this.context = null;
        this.callback = callback;
    }

    Backgrounding(boolean inBackground)
    {
        this.inBackground = inBackground;
        this.context = null;
        this.callback = null;
    }

    Backgrounding(BackgroundCallback callback, Object context)
    {
        this.inBackground = true;
        this.context = context;
        this.callback = callback;
    }

    Backgrounding(CuratorFrameworkImpl client, BackgroundCallback callback, Object context, Executor executor)
    {
        this(wrapCallback(client, callback, executor), context);
    }

    Backgrounding(CuratorFrameworkImpl client, BackgroundCallback callback, Executor executor)
    {
        this(wrapCallback(client, callback, executor));
    }

    Backgrounding()
    {
        inBackground = false;
        context = null;
        this.callback = null;
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
                                client.logError("Background operation result handling threw exception", e);
                            }
                        }
                    }
                );
            }
        };
    }
}
