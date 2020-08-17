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

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;

public class TestZKPaths
{
    @SuppressWarnings("NullArgumentToVariableArgMethod")
    @Test
    public void testMakePath()
    {
        assertEquals(ZKPaths.makePath(null, "/"), "/");
        assertEquals(ZKPaths.makePath("", null), "/");
        assertEquals(ZKPaths.makePath("/", null), "/");
        assertEquals(ZKPaths.makePath(null, null), "/");

        assertEquals(ZKPaths.makePath("/", "/"), "/");
        assertEquals(ZKPaths.makePath("", "/"), "/");
        assertEquals(ZKPaths.makePath("/", ""), "/");
        assertEquals(ZKPaths.makePath("", ""), "/");

        assertEquals(ZKPaths.makePath("foo", ""), "/foo");
        assertEquals(ZKPaths.makePath("foo", "/"), "/foo");
        assertEquals(ZKPaths.makePath("/foo", ""), "/foo");
        assertEquals(ZKPaths.makePath("/foo", "/"), "/foo");

        assertEquals(ZKPaths.makePath("foo", null), "/foo");
        assertEquals(ZKPaths.makePath("foo", null), "/foo");
        assertEquals(ZKPaths.makePath("/foo", null), "/foo");
        assertEquals(ZKPaths.makePath("/foo", null), "/foo");

        assertEquals(ZKPaths.makePath("", "bar"), "/bar");
        assertEquals(ZKPaths.makePath("/", "bar"), "/bar");
        assertEquals(ZKPaths.makePath("", "/bar"), "/bar");
        assertEquals(ZKPaths.makePath("/", "/bar"), "/bar");

        assertEquals(ZKPaths.makePath(null, "bar"), "/bar");
        assertEquals(ZKPaths.makePath(null, "bar"), "/bar");
        assertEquals(ZKPaths.makePath(null, "/bar"), "/bar");
        assertEquals(ZKPaths.makePath(null, "/bar"), "/bar");

        assertEquals(ZKPaths.makePath("foo", "bar"), "/foo/bar");
        assertEquals(ZKPaths.makePath("/foo", "bar"), "/foo/bar");
        assertEquals(ZKPaths.makePath("foo", "/bar"), "/foo/bar");
        assertEquals(ZKPaths.makePath("/foo", "/bar"), "/foo/bar");
        assertEquals(ZKPaths.makePath("/foo", "bar/"), "/foo/bar");
        assertEquals(ZKPaths.makePath("/foo/", "/bar/"), "/foo/bar");

        assertEquals(ZKPaths.makePath("foo", "bar", "baz"), "/foo/bar/baz");
        assertEquals(ZKPaths.makePath("foo", "bar", "baz", "qux"), "/foo/bar/baz/qux");
        assertEquals(ZKPaths.makePath("/foo", "/bar", "/baz"), "/foo/bar/baz");
        assertEquals(ZKPaths.makePath("/foo/", "/bar/", "/baz/"), "/foo/bar/baz");
        assertEquals(ZKPaths.makePath("foo", null, null), "/foo");
        assertEquals(ZKPaths.makePath("foo", "bar", null), "/foo/bar");
        assertEquals(ZKPaths.makePath("foo", null, "baz"), "/foo/baz");
    }

    @Test
    public void testSplit()
    {
        assertEquals(ZKPaths.split("/"), Collections.emptyList());
        assertEquals(ZKPaths.split("/test"), Collections.singletonList("test"));
        assertEquals(ZKPaths.split("/test/one"), Arrays.asList("test", "one"));
        assertEquals(ZKPaths.split("/test/one/two"), Arrays.asList("test", "one", "two"));
    }
}
