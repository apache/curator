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
import org.apache.curator.x.async.modeled.NodeName;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.common.PathUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.curator.utils.ZKPaths.PATH_SEPARATOR;

public class ZPathImpl implements ZPath
{
    public static final ZPath root = new ZPathImpl(Collections.singletonList(PATH_SEPARATOR), null);

    private final List<String> nodes;
    private final boolean isResolved;
    private volatile String fullPath = null;
    private volatile ZPath parent = null;
    private volatile Pattern schema = null;

    public static ZPath parse(String fullPath, UnaryOperator<String> nameFilter)
    {
        return parseInternal(fullPath, nameFilter);
    }

    private static ZPathImpl parseInternal(String fullPath, UnaryOperator<String> nameFilter)
    {
        List<String> nodes = ImmutableList.<String>builder()
            .add(PATH_SEPARATOR)
            .addAll(
                Splitter.on(PATH_SEPARATOR)
                    .omitEmptyStrings()
                    .splitToList(fullPath)
                    .stream()
                    .map(nameFilter)
                    .collect(Collectors.toList())
             )
            .build();
        nodes.forEach(ZPathImpl::validate);
        return new ZPathImpl(nodes, null);
    }

    public static ZPath from(String[] names)
    {
        return from(null, Arrays.asList(names));
    }

    public static ZPath from(List<String> names)
    {
        return from(null, names);
    }

    public static ZPath from(ZPath base, String[] names)
    {
        return from(base, Arrays.asList(names));
    }

    public static ZPath from(ZPath base, List<String> names)
    {
        names = Objects.requireNonNull(names, "names cannot be null");
        names.forEach(ZPathImpl::validate);
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if ( base != null )
        {
            if ( base instanceof ZPathImpl )
            {
                builder.addAll(((ZPathImpl)base).nodes);
            }
            else
            {
                builder.addAll(Splitter.on(PATH_SEPARATOR).omitEmptyStrings().splitToList(base.fullPath()));
            }
        }
        else
        {
            builder.add(PATH_SEPARATOR);
        }
        List<String> nodes = builder.addAll(names).build();
        return new ZPathImpl(nodes, null);
    }

    @Override
    public ZPath child(Object child)
    {
        return new ZPathImpl(nodes, NodeName.nameFrom(child));
    }

    @Override
    public ZPath parent()
    {
        checkRootAccess();
        if ( parent == null )
        {
            parent = new ZPathImpl(nodes.subList(0, nodes.size() - 1), null);
        }
        return parent;
    }

    @Override
    public boolean isRoot()
    {
        return nodes.size() == 1;
    }

    @Override
    public boolean startsWith(ZPath path)
    {
        ZPathImpl rhs;
        if ( path instanceof ZPathImpl )
        {
            rhs = (ZPathImpl)path;
        }
        else
        {
            rhs = parseInternal(path.fullPath(), s -> s);
        }
        return (nodes.size() >= rhs.nodes.size()) && nodes.subList(0, rhs.nodes.size()).equals(rhs.nodes);
    }

    @Override
    public Pattern toSchemaPathPattern()
    {
        if ( schema == null )
        {
            schema = Pattern.compile(buildFullPath(s -> isParameter(s) ? ".*" : s));
        }
        return schema;
    }

    @Override
    public String fullPath()
    {
        checkResolved();
        if ( fullPath == null )
        {
            fullPath = buildFullPath(s -> s);
        }
        return fullPath;
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
        return nodes.subList(1, nodes.size())
            .stream().map(name -> isParameter(name) ? name.substring(1) : name)
            .collect(Collectors.joining(PATH_SEPARATOR, PATH_SEPARATOR, ""));
    }

    @Override
    public ZPath resolved(List<Object> parameters)
    {
        Iterator<Object> iterator = parameters.iterator();
        List<String> nodeNames = nodes.stream()
            .map(name -> {
                if ( isParameter(name) && iterator.hasNext() )
                {
                    return NodeName.nameFrom(iterator.next());
                }
                return name;
            })
            .collect(Collectors.toList());
        return new ZPathImpl(nodeNames, null);
    }

    @Override
    public boolean isResolved()
    {
        return isResolved;
    }

    private static boolean isParameter(String name)
    {
        return (name.length() > 1) && name.startsWith(PATH_SEPARATOR);
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
        isResolved = this.nodes.stream().noneMatch(ZPathImpl::isParameter);
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
        if ( !isResolved)
        {
            throw new IllegalStateException("This ZPath has not been resolved: " + toString());
        }
    }

    private static void validate(String nodeName)
    {
        if ( isParameter(Objects.requireNonNull(nodeName, "nodeName cannot be null")) )
        {
            return;
        }
        if ( nodeName.equals(PATH_SEPARATOR) )
        {
            return;
        }
        PathUtils.validatePath(PATH_SEPARATOR + nodeName);
    }

    private String buildFullPath(UnaryOperator<String> filter)
    {
        boolean addSeparator = false;
        StringBuilder str = new StringBuilder();
        int size = nodes.size();
        int parameterIndex = 0;
        for ( int i = 0; i < size; ++i )
        {
            if ( i > 1 )
            {
                str.append(PATH_SEPARATOR);
            }
            str.append(filter.apply(nodes.get(i)));
        }
        return str.toString();
    }
}
