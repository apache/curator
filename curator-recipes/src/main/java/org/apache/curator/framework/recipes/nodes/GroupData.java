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

import java.util.Arrays;

public class GroupData
{
    private final String id;
    private final byte[] data;

    public GroupData(String id, byte[] data)
    {
        this.id = id;
        this.data = data;
    }

    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        GroupData groupData = (GroupData) o;

        if (!id.equals(groupData.id))
        {
            return false;
        }
        return Arrays.equals(data, groupData.data);
    }

    @Override
    public int hashCode()
    {
        int result = id.hashCode();
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    /**
     * <p>Returns the data associated with this group member</p>
     *
     * <p><b>NOTE:</b> the byte array returned is the raw reference of this instance's field. If you change
     * the values in the array any other callers to this method will see the change.</p>
     *
     * @return node data or null
     */
    public byte[] getData()
    {
        return data;
    }

    /**
     * Returns the id of the group member
     *
     * @return id or null
     */
    public String getId()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return "GroupData{" +
                "id='" + id + '\'' +
                ", data=" + Arrays.toString(data) +
                '}';
    }
}
