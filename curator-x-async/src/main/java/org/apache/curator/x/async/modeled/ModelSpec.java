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

package org.apache.curator.x.async.modeled;

import com.google.common.collect.ImmutableSet;
import org.apache.curator.framework.schema.Schema;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.DeleteOption;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import java.util.List;
import java.util.Set;

/**
 * A full specification for dealing with a portion of the ZooKeeper tree. ModelSpec's contain:
 *
 * <ul>
 *     <li>A node path</li>
 *     <li>Serializer for the data stored</li>
 *     <li>Options for how to create the node (mode, compression, etc.)</li>
 *     <li>Options for how to deleting the node (quietly, guaranteed, etc.)</li>
 *     <li>ACLs</li>
 *     <li>Optional schema generation</li>
 * </ul>
 */
public interface ModelSpec<T> extends Resolvable
{
    Set<CreateOption> defaultCreateOptions = ImmutableSet.of(CreateOption.createParentsAsContainers, CreateOption.setDataIfExists);
    Set<DeleteOption> defaultDeleteOptions = ImmutableSet.of(DeleteOption.guaranteed);

    /**
     * Start a new ModelSpecBuilder for the given path and serializer. The returned ModelSpecBuilder
     * uses {@link #defaultCreateOptions} and {@link #defaultDeleteOptions}, but you can change these
     * with builder methods.
     *
     * @param path path to model
     * @param serializer the model's serializer
     * @return builder
     */
    static <T> ModelSpecBuilder<T> builder(ZPath path, ModelSerializer<T> serializer)
    {
        return new ModelSpecBuilder<>(path, serializer)
            .withCreateOptions(defaultCreateOptions)
            .withDeleteOptions(defaultDeleteOptions);
    }

    /**
     * Start a new ModelSpecBuilder for the given serializer. The returned ModelSpecBuilder
     * uses {@link #defaultCreateOptions} and {@link #defaultDeleteOptions}, but you can change these
     * with builder methods. You must set a path before calling {@link ModelSpecBuilder#build()}
     *
     * @param serializer the model's serializer
     * @return builder
     */
    static <T> ModelSpecBuilder<T> builder(ModelSerializer<T> serializer)
    {
        return new ModelSpecBuilder<>(serializer)
            .withCreateOptions(defaultCreateOptions)
            .withDeleteOptions(defaultDeleteOptions);
    }

    /**
     * <p>
     *     Return a new CuratorModel instance with all the same options but applying to the given child node of this CuratorModel's
     *     path. E.g. if this CuratorModel instance applies to "/a/b", calling <code>modeled.at("c")</code> returns an instance that applies to
     *     "/a/b/c".
     * </p>
     *
     * <p>
     *     The replacement is the <code>toString()</code> value of child or,
     *     if it implements {@link org.apache.curator.x.async.modeled.NodeName},
     *     the value of <code>nodeName()</code>.
     * </p>
     *
     * @param child child node.
     * @return new Modeled Spec instance
     */
    ModelSpec<T> child(Object child);

    /**
     * <p>
     *     Return a new CuratorModel instance with all the same options but applying to the parent node of this CuratorModel's
     *     path. E.g. if this CuratorModel instance applies to "/a/b/c", calling <code>modeled.parent()</code> returns an instance that applies to
     *     "/a/b".
     * </p>
     *
     * <p>
     *     The replacement is the <code>toString()</code> value of child or,
     *     if it implements {@link org.apache.curator.x.async.modeled.NodeName},
     *     the value of <code>nodeName()</code>.
     * </p>
     *
     * @return new Modeled Spec instance
     */
    ModelSpec<T> parent();

    /**
     * Return a new CuratorModel instance with all the same options but using the given path.
     *
     * @param path new path
     * @return new Modeled Spec instance
     */
    ModelSpec<T> withPath(ZPath path);

    /**
     * <p>
     *     Return a new CuratorModel instance with all the same options but using a resolved
     *     path by calling {@link org.apache.curator.x.async.modeled.ZPath#resolved(Object...)}
     *     using the given parameters
     * </p>
     *
     * <p>
     *     The replacement is the <code>toString()</code> value of the parameter object or,
     *     if the object implements {@link org.apache.curator.x.async.modeled.NodeName},
     *     the value of <code>nodeName()</code>.
     * </p>
     *
     * @param parameters list of replacements. Must have be the same length as the number of
     *                   parameter nodes in the path
     * @return new resolved ModelSpec
     */
    @Override
    ModelSpec<T> resolved(Object... parameters);

    /**
     * <p>
     *     Return a new CuratorModel instance with all the same options but using a resolved
     *     path by calling {@link org.apache.curator.x.async.modeled.ZPath#resolved(java.util.List)}
     *     using the given parameters
     * </p>
     *
     * <p>
     *     The replacement is the <code>toString()</code> value of the parameter object or,
     *     if the object implements {@link org.apache.curator.x.async.modeled.NodeName},
     *     the value of <code>nodeName()</code>.
     * </p>
     *
     * @param parameters list of replacements. Must have be the same length as the number of
     *                   parameter nodes in the path
     * @return new resolved ModelSpec
     */
    @Override
    ModelSpec<T> resolved(List<Object> parameters);

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
     * Return the TTL to use or -1
     *
     * @return ttl
     */
    long ttl();

    /**
     * Return a Curator schema that validates ZNodes at this model's
     * path using this model's values
     *
     * @return schema
     */
    Schema schema();
}
