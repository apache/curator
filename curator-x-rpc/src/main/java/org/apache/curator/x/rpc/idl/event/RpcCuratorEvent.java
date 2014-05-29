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
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
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
    }

    public RpcCuratorEvent(CuratorEvent event)
    {
        this.type = toRpcCuratorEventType(event.getType());
        this.resultCode = event.getResultCode();
        this.path = event.getPath();
        this.context = (event.getContext() != null) ? String.valueOf(event.getContext()) : null;
        this.stat = toRpcStat(event.getStat());
        this.data = event.getData();
        this.name = event.getName();
        this.children = event.getChildren();
        this.aclList = toRpcAclList(event.getACLList());
        this.watchedEvent = toRpcWatchedEvent(event.getWatchedEvent());
    }

    public RpcCuratorEvent(ConnectionState newState)
    {
        this.type = toRpcCuratorEventType(newState);
        this.resultCode = 0;
        this.path = null;
        this.context = null;
        this.stat = null;
        this.data = null;
        this.name = null;
        this.children = null;
        this.aclList = null;
        this.watchedEvent = null;
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
        this.watchedEvent = new RpcWatchedEvent(toRpcKeeperState(event.getState()), toRpcEventType(event.getType()), event.getPath());
    }

    private RpcCuratorEventType toRpcCuratorEventType(ConnectionState state)
    {
        switch ( state )
        {
            case CONNECTED:
            {
                return RpcCuratorEventType.CONNECTION_CONNECTED;
            }

            case SUSPENDED:
            {
                return RpcCuratorEventType.CONNECTION_SUSPENDED;
            }

            case RECONNECTED:
            {
                return RpcCuratorEventType.CONNECTION_RECONNECTED;
            }

            case LOST:
            {
                return RpcCuratorEventType.CONNECTION_LOST;
            }

            case READ_ONLY:
            {
                return RpcCuratorEventType.CONNECTION_READ_ONLY;
            }
        }
        throw new IllegalStateException("Unknown state: " + state);
    }

    private RpcCuratorEventType toRpcCuratorEventType(CuratorEventType eventType)
    {
        switch ( eventType )
        {
            case CREATE:
            {
                return RpcCuratorEventType.CREATE;
            }

            case DELETE:
            {
                return RpcCuratorEventType.DELETE;
            }

            case EXISTS:
            {
                return RpcCuratorEventType.EXISTS;
            }

            case GET_DATA:
            {
                return RpcCuratorEventType.GET_DATA;
            }

            case SET_DATA:
            {
                return RpcCuratorEventType.SET_DATA;
            }

            case CHILDREN:
            {
                return RpcCuratorEventType.CHILDREN;
            }

            case SYNC:
            {
                return RpcCuratorEventType.SYNC;
            }

            case GET_ACL:
            {
                return RpcCuratorEventType.GET_ACL;
            }

            case SET_ACL:
            {
                return RpcCuratorEventType.SET_ACL;
            }

            case WATCHED:
            {
                return RpcCuratorEventType.WATCHED;
            }

            case CLOSING:
            {
                return RpcCuratorEventType.CLOSING;
            }
        }

        throw new IllegalStateException("Unknown type: " + eventType);
    }

    public static RpcStat toRpcStat(Stat stat)
    {
        if ( stat != null )
        {
            return new RpcStat
            (
                stat.getCzxid(),
                stat.getMzxid(),
                stat.getCtime(),
                stat.getMtime(),
                stat.getVersion(),
                stat.getCversion(),
                stat.getAversion(),
                stat.getEphemeralOwner(),
                stat.getDataLength(),
                stat.getNumChildren(),
                stat.getPzxid()
            );
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
            RpcKeeperState keeperState = toRpcKeeperState(watchedEvent.getState());
            RpcEventType eventType = toRpcEventType(watchedEvent.getType());
            return new RpcWatchedEvent(keeperState, eventType, watchedEvent.getPath());
        }
        return null;
    }

    private RpcEventType toRpcEventType(Watcher.Event.EventType type)
    {
        switch ( type )
        {
            case None:
            {
                return RpcEventType.None;
            }

            case NodeCreated:
            {
                return RpcEventType.NodeCreated;
            }

            case NodeDeleted:
            {
                return RpcEventType.NodeDeleted;
            }

            case NodeDataChanged:
            {
                return RpcEventType.NodeDataChanged;
            }

            case NodeChildrenChanged:
            {
                return RpcEventType.NodeChildrenChanged;
            }
        }
        throw new IllegalStateException("Unknown type: " + type);
    }

    private RpcKeeperState toRpcKeeperState(Watcher.Event.KeeperState state)
    {
        switch ( state )
        {
            case Unknown:
            {
                return RpcKeeperState.Unknown;
            }

            case Disconnected:
            {
                return RpcKeeperState.Disconnected;
            }

            case NoSyncConnected:
            {
                return RpcKeeperState.NoSyncConnected;
            }

            case SyncConnected:
            {
                return RpcKeeperState.SyncConnected;
            }

            case AuthFailed:
            {
                return RpcKeeperState.AuthFailed;
            }

            case ConnectedReadOnly:
            {
                return RpcKeeperState.ConnectedReadOnly;
            }

            case SaslAuthenticated:
            {
                return RpcKeeperState.SaslAuthenticated;
            }

            case Expired:
            {
                return RpcKeeperState.Expired;
            }
        }
        throw new IllegalStateException("Unknown state: " + state);
    }
}
