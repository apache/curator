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
package org.apache.curator.framework.recipes.leader;

/**
 * Describes a participant in a leadership selection
 */
@SuppressWarnings({"RedundantIfStatement"})
public class Participant
{
    private final String        id;
    private final boolean       isLeader;

    /**
     * @param id the ID
     * @param leader true if the leader
     */
    public Participant(String id, boolean leader)
    {
        this.id = id;
        isLeader = leader;
    }

    Participant()
    {
        this("", false);
    }

    /**
     * Returns the ID set via {@link LeaderSelector#setId(String)}
     *
     * @return id
     */
    public String getId()
    {
        return id;
    }

    /**
     * Returns true if this participant is the current leader
     *
     * @return true/false
     */
    public boolean isLeader()
    {
        return isLeader;
    }

    @Override
    public String toString()
    {
        return "Participant{" +
            "id='" + id + '\'' +
            ", isLeader=" + isLeader +
            '}';
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

        Participant that = (Participant)o;

        if ( isLeader != that.isLeader )
        {
            return false;
        }
        if ( !id.equals(that.id) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = id.hashCode();
        result = 31 * result + (isLeader ? 1 : 0);
        return result;
    }
}
