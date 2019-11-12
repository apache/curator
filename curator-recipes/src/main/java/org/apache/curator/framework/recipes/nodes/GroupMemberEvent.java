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

/**
 * POJO that abstracts a change to a group membership
 */
public class GroupMemberEvent
{
    private final Type type;
    private final GroupData data;

    /**
     * Type of change
     */
    public enum Type
    {
        /**
         * A new member joined group
         */
        MEMBER_JOINED,

        /**
         * An existing member left the group
         */
        MEMBER_LEFT,

        /**
         * The data for a member was updated
         */
        MEMBER_UPDATED
    }

    /**
     * @param type event type
     * @param data event data or null
     */
    public GroupMemberEvent(Type type, GroupData data)
    {
        this.type = type;
        this.data = data;
    }

    /**
     * @return change type
     */
    public Type getType()
    {
        return type;
    }

    /**
     * @return the node's data
     */
    public GroupData getData()
    {
        return data;
    }
}
