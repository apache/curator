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

package org.apache.curator.framework.imps;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

class CuratorEventImpl implements CuratorEvent {
    private final CuratorEventType type;
    private final int resultCode;
    private final String path;
    private final String name;
    private final List<String> children;
    private final Object context;
    private final Stat stat;
    private final byte[] data;
    private final WatchedEvent watchedEvent;
    private final List<ACL> aclList;
    private final List<CuratorTransactionResult> opResults;

    @Override
    public CuratorEventType getType() {
        return type;
    }

    @Override
    public int getResultCode() {
        return resultCode;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public Object getContext() {
        return context;
    }

    @Override
    public Stat getStat() {
        return stat;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<String> getChildren() {
        return children;
    }

    @Override
    public WatchedEvent getWatchedEvent() {
        return watchedEvent;
    }

    @Override
    public List<ACL> getACLList() {
        return aclList;
    }

    @Override
    public List<CuratorTransactionResult> getOpResults() {
        return opResults;
    }

    @Override
    public String toString() {
        return "CuratorEventImpl{" + "type="
                + type + ", resultCode="
                + resultCode + ", path='"
                + path + '\'' + ", name='"
                + name + '\'' + ", children="
                + children + ", context="
                + context + ", stat="
                + stat + ", data="
                + Arrays.toString(data) + ", watchedEvent="
                + watchedEvent + ", aclList="
                + aclList + ", opResults="
                + opResults + '}';
    }

    CuratorEventImpl(
            InternalCuratorFramework client,
            CuratorEventType type,
            int resultCode,
            String path,
            String name,
            Object context,
            Stat stat,
            byte[] data,
            List<String> children,
            WatchedEvent watchedEvent,
            List<ACL> aclList,
            List<CuratorTransactionResult> opResults) {
        this.type = type;
        this.resultCode = resultCode;
        this.opResults = (opResults != null) ? ImmutableList.copyOf(opResults) : null;
        this.path = client.unfixForNamespace(path);
        this.name = client.unfixForNamespace(name);
        this.context = context;
        this.stat = stat;
        this.data = data;
        this.children = children;
        this.watchedEvent = (watchedEvent != null) ? new NamespaceWatchedEvent(client, watchedEvent) : null;
        this.aclList = (aclList != null) ? ImmutableList.copyOf(aclList) : null;
    }
}
