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
package org.apache.curator.x.async.modeled;

import com.google.common.collect.ImmutableSet;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.DeleteOption;
import org.apache.curator.x.async.modeled.details.CuratorModelSpecImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class CuratorModelSpecBuilder<T>
{
    private final ZPath path;
    private final ModelSerializer<T> serializer;
    private CreateMode createMode = CreateMode.PERSISTENT;
    private List<ACL> aclList = Collections.emptyList();
    private Set<CreateOption> createOptions = Collections.emptySet();
    private Set<DeleteOption> deleteOptions = Collections.emptySet();

    /**
     * Build a new CuratorModel instance
     *
     * @return new CuratorModel instance
     */
    public CuratorModelSpec<T> build()
    {
        return new CuratorModelSpecImpl<>(path, serializer, createMode, aclList, createOptions, deleteOptions);
    }

    /**
     * Use the given createMode for create operations on the Modeled Curator's ZNode
     *
     * @param createMode create mode
     * @return this for chaining
     */
    public CuratorModelSpecBuilder<T> withCreateMode(CreateMode createMode)
    {
        this.createMode = createMode;
        return this;
    }

    /**
     * Use the given aclList for create operations on the Modeled Curator's ZNode
     *
     * @param aclList ACLs
     * @return this for chaining
     */
    public CuratorModelSpecBuilder<T> withAclList(List<ACL> aclList)
    {
        this.aclList = aclList;
        return this;
    }

    /**
     * Use the given create options on the Modeled Curator's ZNode
     *
     * @param createOptions options
     * @return this for chaining
     */
    public CuratorModelSpecBuilder<T> withCreateOptions(Set<CreateOption> createOptions)
    {
        this.createOptions = (createOptions != null) ? ImmutableSet.copyOf(createOptions) : null;
        return this;
    }

    /**
     * Use the given delete options on the Modeled Curator's ZNode
     *
     * @param deleteOptions options
     * @return this for chaining
     */
    public CuratorModelSpecBuilder<T> withDeleteOptions(Set<DeleteOption> deleteOptions)
    {
        this.deleteOptions = (deleteOptions != null) ? ImmutableSet.copyOf(deleteOptions) : null;
        return this;
    }

    CuratorModelSpecBuilder(ZPath path, ModelSerializer<T> serializer)
    {
        this.path = Objects.requireNonNull(path, "path cannot be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer cannot be null");
    }
}
