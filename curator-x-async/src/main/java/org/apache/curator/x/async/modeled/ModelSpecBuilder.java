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
import org.apache.curator.x.async.modeled.details.ModelSpecImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class ModelSpecBuilder<T>
{
    private final ModelSerializer<T> serializer;
    private ZPath path;
    private CreateMode createMode = CreateMode.PERSISTENT;
    private List<ACL> aclList = Collections.emptyList();
    private Set<CreateOption> createOptions = Collections.emptySet();
    private Set<DeleteOption> deleteOptions = Collections.emptySet();
    private Function<T, String> nodeName = Object::toString;

    /**
     * Build a new ModelSpec instance
     *
     * @return new ModelSpec instance
     */
    public ModelSpec<T> build()
    {
        return new ModelSpecImpl<>(path, serializer, createMode, aclList, createOptions, deleteOptions, nodeName);
    }

    /**
     * Use the given createMode for create operations on the Modeled Curator's ZNode
     *
     * @param createMode create mode
     * @return this for chaining
     */
    public ModelSpecBuilder<T> withCreateMode(CreateMode createMode)
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
    public ModelSpecBuilder<T> withAclList(List<ACL> aclList)
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
    public ModelSpecBuilder<T> withCreateOptions(Set<CreateOption> createOptions)
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
    public ModelSpecBuilder<T> withDeleteOptions(Set<DeleteOption> deleteOptions)
    {
        this.deleteOptions = (deleteOptions != null) ? ImmutableSet.copyOf(deleteOptions) : null;
        return this;
    }

    /**
     * Functor that returns the node name to use for a model instance. Default is to call
     * <code>toString()</code> on the model instance.
     *
     * @param nodeName naming functor
     * @return this for chaining
     */
    public ModelSpecBuilder<T> withNodeName(Function<T, String> nodeName)
    {
        this.nodeName = Objects.requireNonNull(nodeName, "nodeName cannot be null");
        return this;
    }

    /**
     * Change the model spec's path
     *
     * @param path new path
     * @return this for chaining
     */
    public ModelSpecBuilder<T> withPath(ZPath path)
    {
        this.path = Objects.requireNonNull(path, "path cannot be null");
        return this;
    }

    ModelSpecBuilder(ModelSerializer<T> serializer)
    {
        this.serializer = Objects.requireNonNull(serializer, "serializer cannot be null");
    }

    ModelSpecBuilder(ZPath path, ModelSerializer<T> serializer)
    {
        this.path = Objects.requireNonNull(path, "path cannot be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer cannot be null");
    }
}
