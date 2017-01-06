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
package org.apache.curator.x.async.details;

import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.imps.Backgrounding;
import org.apache.curator.x.async.WatchMode;

class BuilderCommon<T>
{
    final InternalCallback<T> internalCallback;
    final Backgrounding backgrounding;
    final InternalWatcher watcher;

    BuilderCommon(UnhandledErrorListener unhandledErrorListener, BackgroundProc<T> proc)
    {
        this(unhandledErrorListener, null, proc);
    }

    BuilderCommon(UnhandledErrorListener unhandledErrorListener, WatchMode watchMode, BackgroundProc<T> proc)
    {
        watcher = (watchMode != null) ? new InternalWatcher(watchMode) : null;
        internalCallback = new InternalCallback<>(proc, watcher);
        backgrounding = new Backgrounding(internalCallback, unhandledErrorListener);
    }
}
