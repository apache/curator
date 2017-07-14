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

import org.apache.curator.framework.api.transaction.CuratorOp;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Models a single migration/transition
 */
public interface Migration
{
    /**
     * @return the unique ID for this migration
     */
    String id();

    /**
     * @return the version of this migration
     */
    int version();

    /**
     * @return the operations to execute in a transaction
     */
    List<CuratorOp> operations();

    static Migration build(String id, Supplier<List<CuratorOp>> operationsProc)
    {
        return build(id, 1, operationsProc);
    }

    static Migration build(String id, int version, Supplier<List<CuratorOp>> operationsProc)
    {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(operationsProc, "operationsProc cannot be null");
        return new Migration()
        {
            @Override
            public String id()
            {
                return id;
            }

            @Override
            public int version()
            {
                return version;
            }

            @Override
            public List<CuratorOp> operations()
            {
                return operationsProc.get();
            }
        };
    }
}
