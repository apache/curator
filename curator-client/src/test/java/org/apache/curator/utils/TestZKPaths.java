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

package org.apache.curator.utils;

import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.Collections;

public class TestZKPaths
{
    @Test
    public void testFixForNamespaceWithEmptyNodeName()
    {
        Assert.assertEquals(ZKPaths.fixForNamespace("foo", "/bar/", true, false), "/foo/bar");
    }

    @Test
    public void testFixForNamespaceWithEmptyNodeNameAllowed()
    {
        Assert.assertEquals(ZKPaths.fixForNamespace("foo", "/bar/", true, true), "/foo/bar/");
    }

    @SuppressWarnings("NullArgumentToVariableArgMethod")
    @Test
    public void testMakePath()
    {
        Assert.assertEquals(ZKPaths.makePath(null, "/"), "/");
        Assert.assertEquals(ZKPaths.makePath("", null), "/");
        Assert.assertEquals(ZKPaths.makePath("/", null), "/");
        Assert.assertEquals(ZKPaths.makePath(null, null), "/");

        Assert.assertEquals(ZKPaths.makePath("/", "/"), "/");
        Assert.assertEquals(ZKPaths.makePath("", "/"), "/");
        Assert.assertEquals(ZKPaths.makePath("/", ""), "/");
        Assert.assertEquals(ZKPaths.makePath("", ""), "/");

        Assert.assertEquals(ZKPaths.makePath("foo", ""), "/foo");
        Assert.assertEquals(ZKPaths.makePath("foo", "/"), "/foo");
        Assert.assertEquals(ZKPaths.makePath("/foo", ""), "/foo");
        Assert.assertEquals(ZKPaths.makePath("/foo", "/"), "/foo");

        Assert.assertEquals(ZKPaths.makePath("foo", null), "/foo");
        Assert.assertEquals(ZKPaths.makePath("foo", null), "/foo");
        Assert.assertEquals(ZKPaths.makePath("/foo", null), "/foo");
        Assert.assertEquals(ZKPaths.makePath("/foo", null), "/foo");

        Assert.assertEquals(ZKPaths.makePath("", "bar"), "/bar");
        Assert.assertEquals(ZKPaths.makePath("/", "bar"), "/bar");
        Assert.assertEquals(ZKPaths.makePath("", "/bar"), "/bar");
        Assert.assertEquals(ZKPaths.makePath("/", "/bar"), "/bar");

        Assert.assertEquals(ZKPaths.makePath(null, "bar"), "/bar");
        Assert.assertEquals(ZKPaths.makePath(null, "bar"), "/bar");
        Assert.assertEquals(ZKPaths.makePath(null, "/bar"), "/bar");
        Assert.assertEquals(ZKPaths.makePath(null, "/bar"), "/bar");

        Assert.assertEquals(ZKPaths.makePath("foo", "bar"), "/foo/bar");
        Assert.assertEquals(ZKPaths.makePath("/foo", "bar"), "/foo/bar");
        Assert.assertEquals(ZKPaths.makePath("foo", "/bar"), "/foo/bar");
        Assert.assertEquals(ZKPaths.makePath("/foo", "/bar"), "/foo/bar");
        Assert.assertEquals(ZKPaths.makePath("/foo", "bar/"), "/foo/bar");
        Assert.assertEquals(ZKPaths.makePath("/foo/", "/bar/"), "/foo/bar");

        Assert.assertEquals(ZKPaths.makePath("foo", "bar", "baz"), "/foo/bar/baz");
        Assert.assertEquals(ZKPaths.makePath("foo", "bar", "baz", "qux"), "/foo/bar/baz/qux");
        Assert.assertEquals(ZKPaths.makePath("/foo", "/bar", "/baz"), "/foo/bar/baz");
        Assert.assertEquals(ZKPaths.makePath("/foo/", "/bar/", "/baz/"), "/foo/bar/baz");
        Assert.assertEquals(ZKPaths.makePath("foo", null, null), "/foo");
        Assert.assertEquals(ZKPaths.makePath("foo", "bar", null), "/foo/bar");
        Assert.assertEquals(ZKPaths.makePath("foo", null, "baz"), "/foo/baz");
    }

    @Test
    public void testSplit()
    {
        Assert.assertEquals(ZKPaths.split("/"), Collections.emptyList());
        Assert.assertEquals(ZKPaths.split("/test"), Collections.singletonList("test"));
        Assert.assertEquals(ZKPaths.split("/test/one"), Arrays.asList("test", "one"));
        Assert.assertEquals(ZKPaths.split("/test/one/two"), Arrays.asList("test", "one", "two"));
    }
}
