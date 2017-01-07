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

import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.zookeeper.WatchedEvent;
import java.util.function.UnaryOperator;

public class Filters
{
    private final UnhandledErrorListener listener;
    private final UnaryOperator<CuratorEvent> resultFilter;
    private final UnaryOperator<WatchedEvent> watcherFilter;

    public Filters(UnhandledErrorListener listener, UnaryOperator<CuratorEvent> resultFilter, UnaryOperator<WatchedEvent> watcherFilter)
    {
        this.listener = listener;
        this.resultFilter = resultFilter;
        this.watcherFilter = watcherFilter;
    }

    public UnhandledErrorListener getListener()
    {
        return listener;
    }

    public UnaryOperator<CuratorEvent> getResultFilter()
    {
        return resultFilter;
    }

    public UnaryOperator<WatchedEvent> getWatcherFilter()
    {
        return watcherFilter;
    }
}
