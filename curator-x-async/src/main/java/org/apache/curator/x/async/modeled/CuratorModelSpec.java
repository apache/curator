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
import org.apache.curator.framework.schema.Schema;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.DeleteOption;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public interface CuratorModelSpec<T>
{
    Set<CreateOption> defaultCreateOptions = ImmutableSet.of(CreateOption.createParentsAsContainers, CreateOption.setDataIfExists);
    Set<DeleteOption> defaultDeleteOptions = ImmutableSet.of(DeleteOption.guaranteed);

    /**
     * Start a new CuratorModelBuilder for the given path and serializer. The returned CuratorModelBuilder
     * uses {@link #defaultCreateOptions} and {@link #defaultDeleteOptions}, but you can change these
     * with builder methods.
     *
     * @param path path to model
     * @param serializer the model's serializer
     * @return builder
     */
    static <T> CuratorModelSpecBuilder<T> builder(ZPath path, ModelSerializer<T> serializer)
    {
        return new CuratorModelSpecBuilder<>(path, serializer)
            .withCreateOptions(defaultCreateOptions)
            .withDeleteOptions(defaultDeleteOptions);
    }

    /**
     * Return a new CuratorModel instance with all the same options but applying to the given child node of this CuratorModel's
     * path. E.g. if this CuratorModel instance applies to "/a/b", calling <code>modeled.at("c")</code> returns an instance that applies to
     * "/a/b/c".
     *
     * @param child child node.
     * @return new Modeled Curator instance
     */
    CuratorModelSpec<T> at(String child);

    /**
     * Return a new CuratorModel instance with all the same options but applying to the given parameters of this CuratorModel's
     * path via {@link org.apache.curator.x.async.modeled.ZPath#resolved(Object...)}
     *
     * @param parameters list of replacements. Must have be the same length as the number of
     *                   parameter nodes in the path
     * @return new Modeled Curator instance
     */
    CuratorModelSpec<T> resolved(Object... parameters);

    /**
     * Return a new CuratorModel instance with all the same options but applying to the given parameters of this CuratorModel's
     * path via {@link org.apache.curator.x.async.modeled.ZPath#resolved(Object...)}
     *
     * @param parameters list of replacements. Must have be the same length as the number of
     *                   parameter nodes in the path
     * @return new Modeled Curator instance
     */
    CuratorModelSpec<T> resolved(List<Object> parameters);

    /**
     * An "auto" resolving version of this CuratorModel. i.e. if any of the path names is
     * the {@link org.apache.curator.x.async.modeled.ZPath#parameterNodeName()} the ZPath must be resolved. This method
     * creates a new CuratorModelSpec that auto resolves by using the given parameter suppliers
     * whenever needed.
     *
     * @param parameterSuppliers parameter suppliers
     * @return new auto resolving ZNode
     * @see #resolved(Object...)
     * @see org.apache.curator.x.async.modeled.ZPath#parameterNodeName()
     */
    CuratorModelSpec<T> resolving(List<Supplier<Object>> parameterSuppliers);

    /**
     * Return the model's path
     *
     * @return path
     */
    ZPath path();

    /**
     * Return the model's serializer
     *
     * @return serializer
     */
    ModelSerializer<T> serializer();

    /**
     * Return the model's create mode
     *
     * @return create mode
     */
    CreateMode createMode();

    /**
     * Return the model's ACL list
     *
     * @return ACL list
     */
    List<ACL> aclList();

    /**
     * Return the model's create options
     *
     * @return create options
     */
    Set<CreateOption> createOptions();

    /**
     * Return the model's delete options
     *
     * @return delete options
     */
    Set<DeleteOption> deleteOptions();

    /**
     * Return a Curator schema that validates ZNodes at this model's
     * path using this model's values
     *
     * @return schema
     */
    Schema schema();
}
