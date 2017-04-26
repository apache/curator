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
package org.apache.curator.x.async.modeled.details;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.common.PathUtils;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Pattern;

public class ZPathImpl implements ZPath
{
    public static final ZPath root = new ZPathImpl(Collections.singletonList(ZKPaths.PATH_SEPARATOR), null);

    public static final String parameter = "\u0000";    // PathUtils.validatePath() rejects this so it's useful for this purpose

    private final List<String> nodes;
    private final boolean isResolved;

    public static ZPath parse(String fullPath)
    {
        List<String> nodes = ImmutableList.<String>builder()
            .add(ZKPaths.PATH_SEPARATOR)
            .addAll(Splitter.on(ZKPaths.PATH_SEPARATOR).omitEmptyStrings().splitToList(fullPath))
            .build();
        nodes.forEach(ZPathImpl::validate);
        return new ZPathImpl(nodes, null);
    }

    @Override
    public ZPath at(String child)
    {
        return new ZPathImpl(nodes, child);
    }

    @Override
    public ZPath parent()
    {
        checkRootAccess();
        return new ZPathImpl(nodes.subList(0, nodes.size() - 1), null);
    }

    @Override
    public boolean isRoot()
    {
        return nodes.size() == 1;
    }

    @Override
    public Pattern toSchemaPathPattern()
    {
        return Pattern.compile(fullPath() + ZKPaths.PATH_SEPARATOR + ".*");
    }

    @Override
    public String fullPath()
    {
        checkResolved();
        return buildFullPath(false);
    }

    @Override
    public String parentPath()
    {
        checkRootAccess();
        checkResolved();
        return buildFullPath(true);
    }

    @Override
    public String nodeName()
    {
        checkResolved();
        return nodes.get(nodes.size() - 1);
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

        ZPathImpl zPaths = (ZPathImpl)o;

        return nodes.equals(zPaths.nodes);
    }

    @Override
    public int hashCode()
    {
        return nodes.hashCode();
    }

    @Override
    public String toString()
    {
        return "ZPathImpl{" + "nodes=" + nodes + '}';
    }

    @Override
    public ZPath resolved(Object... parameters)
    {
        List<String> nodeNames = Lists.newArrayList();
        for ( int i = 0; i < nodes.size(); ++i )
        {
            String name = nodes.get(i);
            if ( name.equals(parameter) )
            {
                if ( parameters.length >= i )
                {
                    throw new IllegalStateException(String.format("Parameter missing at index [%d] for [%s]", i, nodes.toString()));
                }
                nodeNames.add(parameters[i].toString());
            }
            else
            {
                nodeNames.add(name);
            }
        }
        return new ZPathImpl(nodeNames, null);
    }

    private ZPathImpl(List<String> nodes, String child)
    {
        ImmutableList.Builder<String> builder = ImmutableList.<String>builder().addAll(nodes);
        if ( child != null )
        {
            validate(child);
            builder.add(child);
        }
        this.nodes = builder.build();
        isResolved = !this.nodes.contains(parameter);
    }

    private void checkRootAccess()
    {
        if ( isRoot() )
        {
            throw new NoSuchElementException("The root has no parent");
        }
    }

    private void checkResolved()
    {
        Preconditions.checkState(isResolved, "This ZPath has not been resolved");
    }

    private String buildFullPath(boolean parent)
    {
        boolean addSeparator = false;
        StringBuilder str = new StringBuilder();
        int size = parent ? (nodes.size() - 1) : nodes.size();
        for ( int i = 0; i < size; ++i )
        {
            if ( i > 1 )
            {
                str.append(ZKPaths.PATH_SEPARATOR);
            }
            String value = nodes.get(i);
            str.append(value.equals(parameter) ? ".*" : value);
        }
        return str.toString();
    }

    private static void validate(String nodeName)
    {
        if ( parameter.equals(Objects.requireNonNull(nodeName, "nodeName cannot be null")) )
        {
            return;
        }
        if ( nodeName.equals(ZKPaths.PATH_SEPARATOR) )
        {
            return;
        }
        PathUtils.validatePath(ZKPaths.PATH_SEPARATOR + nodeName);
    }
}
