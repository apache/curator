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

package org.apache.curator.x.rest.system;

import com.google.common.base.Preconditions;
import java.util.UUID;

public class ThingKey<T>
{
    private final String id;
    private final ThingType<T> type;

    public ThingKey(ThingType<T> type)
    {
        this(UUID.randomUUID().toString(), type);
    }

    public ThingKey(String id, ThingType<T> type)
    {
        this.id = Preconditions.checkNotNull(id, "id cannot be null");
        this.type = Preconditions.checkNotNull(type, "type cannot be null");
    }

    public String getId()
    {
        return id;
    }

    public ThingType<T> getType()
    {
        return type;
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

        ThingKey thingKey = (ThingKey)o;

        if ( !id.equals(thingKey.id) )
        {
            return false;
        }
        //noinspection RedundantIfStatement
        if ( type != thingKey.type )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = id.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
