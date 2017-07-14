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
package org.apache.curator.x.async.modeled.migrations;

import java.util.Objects;

/**
 * The meta data of a single migration
 */
public class MetaData
{
    private final String migrationId;
    private final int migrationVersion;

    public MetaData()
    {
        this("", 0);
    }

    public MetaData(String migrationId, int migrationVersion)
    {
        this.migrationId = Objects.requireNonNull(migrationId, "migrationId cannot be null");
        this.migrationVersion = migrationVersion;
    }

    /**
     * @return The ID of the migration that was applied
     */
    public String getMigrationId()
    {
        return migrationId;
    }

    /**
     * @return the version of the migration that was applied
     */
    public int getMigrationVersion()
    {
        return migrationVersion;
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

        MetaData metaData = (MetaData)o;

        //noinspection SimplifiableIfStatement
        if ( migrationVersion != metaData.migrationVersion )
        {
            return false;
        }
        return migrationId.equals(metaData.migrationId);
    }

    @Override
    public int hashCode()
    {
        int result = migrationId.hashCode();
        result = 31 * result + migrationVersion;
        return result;
    }

    @Override
    public String toString()
    {
        return "MetaData{" + "migrationId='" + migrationId + '\'' + ", migrationVersion=" + migrationVersion + '}';
    }
}
