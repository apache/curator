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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.common.PathUtils;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class ZPathImpl implements ZPath
{
    public static final ZPath root = new ZPathImpl(Collections.singletonList(ZKPaths.PATH_SEPARATOR));

    private volatile String fullPathCache = null;
    private volatile String parentPathCache = null;

    private final List<String> nodes;

    public static ZPath parse(String fullPath)
    {
        PathUtils.validatePath(fullPath);
        List<String> nodes = ImmutableList.<String>builder()
            .add(ZKPaths.PATH_SEPARATOR)
            .addAll(Splitter.on(ZKPaths.PATH_SEPARATOR).omitEmptyStrings().splitToList(fullPath))
            .build();
        return new ZPathImpl(nodes);
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
        return new ZPathImpl(nodes.subList(0, nodes.size() - 1));
    }

    @Override
    public boolean isRoot()
    {
        return nodes.size() == 1;
    }

    @Override
    public String fullPath()
    {
        return buildFullPath(false);
    }

    @Override
    public String parentPath()
    {
        checkRootAccess();
        return buildFullPath(true);
    }

    @Override
    public String nodeName()
    {
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

    private ZPathImpl(List<String> nodes)
    {
        this.nodes = Objects.requireNonNull(nodes, "nodes cannot be null");
    }

    private ZPathImpl(List<String> nodes, String child)
    {
        PathUtils.validatePath(ZKPaths.PATH_SEPARATOR + child);
        this.nodes = ImmutableList.<String>builder()
            .addAll(nodes)
            .add(child)
            .build();
    }

    private void checkRootAccess()
    {
        if ( isRoot() )
        {
            throw new NoSuchElementException("The root has no parent");
        }
    }

    private String buildFullPath(boolean parent)
    {
        String path = parent ? parentPathCache : fullPathCache;
        if ( path != null )
        {
            return path;
        }

        boolean addSeparator = false;
        StringBuilder str = new StringBuilder();
        int size = parent ? (nodes.size() - 1) : nodes.size();
        for ( int i = 0; i < size; ++i )
        {
            if ( i > 1 )
            {
                str.append(ZKPaths.PATH_SEPARATOR);
            }
            str.append(nodes.get(i));
        }
        path = str.toString();

        if ( parent )
        {
            parentPathCache = path;
        }
        else
        {
            fullPathCache = path;
        }
        return path;
    }
}
