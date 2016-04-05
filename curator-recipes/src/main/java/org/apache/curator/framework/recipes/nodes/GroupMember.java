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
package org.apache.curator.framework.recipes.nodes;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;

/**
 * Group membership management. Adds this instance into a group and
 * keeps a cache of members in the group
 */
public class GroupMember implements Closeable {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final PersistentEphemeralNode pen;
    private final PathChildrenCache cache;
    private final String thisId;
    private ListenerContainer<GroupMemberListener> listeners = new ListenerContainer<GroupMemberListener>();
    private final PathChildrenCacheListener childrenCacheListener = new PathChildrenCacheListener()
    {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            ChildData data = event.getData();
            GroupData groupData = new GroupData(idFromPath(data.getPath()), data.getData());

            switch( event.getType() )
            {
            case CHILD_ADDED:
            {
                callListeners(new GroupMemberEvent(GroupMemberEvent.Type.MEMBER_JOINED, groupData));
                break;
            }

            case CHILD_REMOVED:
            {
                callListeners(new GroupMemberEvent(GroupMemberEvent.Type.MEMBER_LEFT, groupData));
                break;
            }

            case CHILD_UPDATED:
            {
                callListeners(new GroupMemberEvent(GroupMemberEvent.Type.MEMBER_UPDATED, groupData));
                break;
            }

            default:
            {
                break;
            }
            }
        }
    };

    /**
     * @param client client
     * @param membershipPath the path to use for membership
     * @param thisId ID of this group member. MUST be unique for the group
     */
    public GroupMember(CuratorFramework client, String membershipPath, String thisId)
    {
        this(client, membershipPath, thisId, CuratorFrameworkFactory.getLocalAddress());
    }

    /**
     * @param client client
     * @param membershipPath the path to use for membership
     * @param thisId ID of this group member. MUST be unique for the group
     * @param payload the payload to write in our member node
     */
    public GroupMember(CuratorFramework client, String membershipPath, String thisId, byte[] payload)
    {
        this.client = client;
        this.thisId = Preconditions.checkNotNull(thisId, "thisId cannot be null");

        cache = newPathChildrenCache(client, membershipPath);
        pen = newPersistentEphemeralNode(client, membershipPath, thisId, payload);
    }

    /**
     * Start the group membership. Register thisId as a member and begin
     * caching all members
     */
    public void start()
    {
        pen.start();
        try
        {
            cache.start();
            cache.getListenable().addListener(childrenCacheListener);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            Throwables.propagate(e);
        }
    }

    /**
     * Change the data stored in this instance's node
     *
     * @param data new data (cannot be null)
     */
    public void setThisData(byte[] data)
    {
        try
        {
            pen.setData(data);
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            Throwables.propagate(e);
        }
    }

    /**
     * Return the group member listenable
     *
     * @return listenable
     */
    public ListenerContainer<GroupMemberListener> getListenable()
    {
        return listeners;
    }

    /**
     * Default behavior is just to log the exception
     *
     * @param e the exception
     */
    protected void handleException(Throwable e)
    {
        log.error("", e);
    }

    void callListeners(final GroupMemberEvent event)
    {
        listeners.forEach
            (
                new Function<GroupMemberListener, Void>()
                {
                    @Override
                    public Void apply(GroupMemberListener listener)
                    {
                        try
                        {
                            listener.groupEvent(client, event);
                        }
                        catch ( Exception e )
                        {
                            ThreadUtils.checkInterrupted(e);
                            handleException(e);
                        }
                        return null;
                    }
                }
            );
    }

    /**
     * Have thisId leave the group and stop caching membership
     */
    @Override
    public void close()
    {
        CloseableUtils.closeQuietly(cache);
        CloseableUtils.closeQuietly(pen);
    }

    /**
     * Return the current view of membership. The keys are the IDs
     * of the members. The values are each member's payload
     *
     * @return membership
     */
    public Map<String, byte[]> getCurrentMembers()
    {
        ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
        boolean thisIdAdded = false;
        for ( ChildData data : cache.getCurrentData() )
        {
            String id = idFromPath(data.getPath());
            thisIdAdded = thisIdAdded || id.equals(thisId);
            builder.put(id, data.getData());
        }
        if ( !thisIdAdded )
        {
            builder.put(thisId, pen.getData());   // this instance is always a member
        }
        return builder.build();
    }

    /**
     * Given a full ZNode path, return the member ID
     *
     * @param path full ZNode path
     * @return id
     */
    public String idFromPath(String path)
    {
        return ZKPaths.getNodeFromPath(path);
    }

    protected PersistentEphemeralNode newPersistentEphemeralNode(CuratorFramework client, String membershipPath, String thisId, byte[] payload)
    {
        return new PersistentEphemeralNode(client, PersistentEphemeralNode.Mode.EPHEMERAL, ZKPaths.makePath(membershipPath, thisId), payload);
    }

    protected PathChildrenCache newPathChildrenCache(CuratorFramework client, String membershipPath)
    {
        return new PathChildrenCache(client, membershipPath, true);
    }
}
