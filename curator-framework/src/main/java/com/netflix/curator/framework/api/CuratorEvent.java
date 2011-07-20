/*
 Copyright 2011 Netflix, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package com.netflix.curator.framework.api;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;

/**
 * A super set of all the various Zookeeper events/background methods.
 *
 * IMPORTANT: the methods only return values as specified by the operation that generated them. Many methods
 * will return <tt>null</tt>
 */
public interface CuratorEvent
{
    /**
     * check here first - this value determines the type of event and which methods will have
     * valid values
     *
     * @return event type
     */
    public CuratorEventType getType();

    /**
     * @return "rc" from async callbacks
     */
    public int getResultCode();

    /**
     * @return the path
     */
    public String getPath();

    /**
     * @return the context object passed to {@link Backgroundable#inBackground(Object)}
     */
    public Object getContext();

    /**
     * @return any stat
     */
    public Stat getStat();

    /**
     * @return any data
     */
    public byte[] getData();

    /**
     * @return any name
     */
    public String getName();

    /**
     * @return any children
     */
    public List<String> getChildren();

    /**
     * @return any ACL list or null
     */
    public List<ACL> getACLList();

    /**
     * If {@link #getType()} returns {@link CuratorEventType#WATCHED} this will
     * return the WatchedEvent
     *
     * @return any WatchedEvent
     */
    public WatchedEvent getWatchedEvent();
}
