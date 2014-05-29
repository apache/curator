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
package org.apache.curator.x.rpc.idl.event;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import javax.annotation.Nullable;
import java.util.List;

@SuppressWarnings("deprecation")
@ThriftStruct("CuratorEvent")
public class RpcCuratorEvent
{
    @ThriftField(2)
    public RpcCuratorEventType type;

    @ThriftField(3)
    public int resultCode;

    @ThriftField(4)
    public String path;

    @ThriftField(5)
    public String context;

    @ThriftField(6)
    public RpcStat stat;

    @ThriftField(7)
    public byte[] data;

    @ThriftField(8)
    public String name;

    @ThriftField(9)
    public List<String> children;

    @ThriftField(10)
    public List<RpcAcl> aclList;

    @ThriftField(11)
    public RpcWatchedEvent watchedEvent;

    @ThriftField(12)
    public LeaderEvent leaderEvent;

    public RpcCuratorEvent()
    {
        this.type = RpcCuratorEventType.PING;
        this.resultCode = 0;
        this.path = null;
        this.context = null;
        this.stat = null;
        this.data = null;
        this.name = null;
        this.children = null;
        this.aclList = null;
        this.watchedEvent = null;
        this.leaderEvent = null;
    }

    public RpcCuratorEvent(CuratorEvent event)
    {
        this.type = RpcCuratorEventType.valueOf(event.getType().name());
        this.resultCode = event.getResultCode();
        this.path = event.getPath();
        this.context = (event.getContext() != null) ? String.valueOf(event.getContext()) : null;
        this.stat = toRpcStat(event.getStat());
        this.data = event.getData();
        this.name = event.getName();
        this.children = event.getChildren();
        this.aclList = toRpcAclList(event.getACLList());
        this.watchedEvent = toRpcWatchedEvent(event.getWatchedEvent());
        this.leaderEvent = null;
    }

    public RpcCuratorEvent(ConnectionState newState)
    {
        this.type = RpcCuratorEventType.valueOf(newState.name());
        this.resultCode = 0;
        this.path = null;
        this.context = null;
        this.stat = null;
        this.data = null;
        this.name = null;
        this.children = null;
        this.aclList = null;
        this.watchedEvent = null;
        this.leaderEvent = null;
    }

    public RpcCuratorEvent(WatchedEvent event)
    {
        this.type = RpcCuratorEventType.WATCHED;
        this.resultCode = 0;
        this.path = event.getPath();
        this.context = null;
        this.stat = null;
        this.data = null;
        this.name = null;
        this.children = null;
        this.aclList = null;
        this.watchedEvent = new RpcWatchedEvent(RpcKeeperState.valueOf(event.getState().name()), RpcEventType.valueOf(event.getType().name()), event.getPath());
        this.leaderEvent = null;
    }

    public RpcCuratorEvent(LeaderEvent event)
    {
        this.type = RpcCuratorEventType.LEADER;
        this.resultCode = 0;
        this.path = event.path;
        this.context = null;
        this.stat = null;
        this.data = null;
        this.name = null;
        this.children = null;
        this.aclList = null;
        this.watchedEvent = null;
        this.leaderEvent = event;
    }

    public static RpcStat toRpcStat(Stat stat)
    {
        if ( stat != null )
        {
            return new RpcStat(stat);
        }
        return null;
    }

    private List<RpcAcl> toRpcAclList(List<ACL> aclList)
    {
        if ( aclList != null )
        {
            return Lists.transform
            (
                aclList,
                new Function<ACL, RpcAcl>()
                {
                    @Nullable
                    @Override
                    public RpcAcl apply(ACL acl)
                    {
                        RpcId id = new RpcId(acl.getId().getScheme(), acl.getId().getId());
                        return new RpcAcl(acl.getPerms(), id);
                    }
                }
            );
        }
        return null;
    }

    private RpcWatchedEvent toRpcWatchedEvent(WatchedEvent watchedEvent)
    {
        if ( watchedEvent != null )
        {
            return new RpcWatchedEvent(watchedEvent);
        }
        return null;
    }
}
