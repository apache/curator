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

package org.apache.curator.x.async.migrations;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

/**
 * Models a set of migrations. Each individual migration is applied
 * in a transaction.
 */
public interface MigrationSet
{
    /**
     * @return the unique ID for this migration set
     */
    String id();

    /**
     * @return list of migrations in the order that they should be applied
     */
    List<Migration> migrations();

    static MigrationSet build(String id, List<Migration> migrations)
    {
        Objects.requireNonNull(id, "id cannot be null");
        final List<Migration> migrationsCopy = ImmutableList.copyOf(migrations);
        return new MigrationSet()
        {
            @Override
            public String id()
            {
                return id;
            }

            @Override
            public List<Migration> migrations()
            {
                return migrationsCopy;
            }
        };
    }
}
