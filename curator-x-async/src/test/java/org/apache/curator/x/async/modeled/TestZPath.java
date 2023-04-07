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

import static org.apache.curator.x.async.modeled.ZPath.parameter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.async.modeled.details.ZPathImpl;
import org.junit.jupiter.api.Test;

public class TestZPath
{
    @Test
    public void testRoot()
    {
        assertEquals(ZPath.root.nodeName(), ZKPaths.PATH_SEPARATOR);
        assertEquals(ZPath.root, ZPathImpl.root);
        assertTrue(ZPath.root.isRoot());
        assertEquals(ZPath.root.child("foo").parent(), ZPath.root);
        assertTrue(ZPath.root.child("foo").parent().isRoot());
    }

    @Test
    public void testBasic()
    {
        ZPath path = ZPath.root.child("one").child("two");
        assertFalse(path.isRoot());
        assertEquals(path, ZPath.root.child("one").child("two"));
        assertNotEquals(path, ZPath.root.child("onex").child("two"));
        assertEquals(path.nodeName(), "two");
        assertEquals(path.fullPath(), "/one/two");
        assertEquals(path.parent().fullPath(), "/one");
        assertEquals(path.fullPath(), "/one/two");       // call twice to test the internal cache
        assertEquals(path.parent().fullPath(), "/one");  // call twice to test the internal cache

        assertTrue(path.startsWith(ZPath.root.child("one")));
        assertFalse(path.startsWith(ZPath.root.child("two")));

        // Despite these paths containing elements which appear to be parameters, ZPath.parse() always returns
        // a ZPath which is considered fully resolved.  This allows users to include parameter-like elements in their
        // ZPath's that aren't treated as parameters.
        ZPath checkIdLike = ZPath.parse("/one/{two}/three");
        assertTrue(checkIdLike.isResolved(), "parse method always returns a fully resolved ZPath");
        checkIdLike = ZPath.parse("/one/" + ZPath.parameter() + "/three");
        assertTrue(checkIdLike.isResolved(), "parse method always returns a fully resolved ZPath");
        checkIdLike = ZPath.parse("/one/" + ZPath.parameter("others") + "/three");
        assertTrue(checkIdLike.isResolved(), "parse method always returns a fully resolved ZPath");
    }

    @Test
    public void testParsing()
    {
        assertEquals(ZPath.parse("/"), ZPath.root);
        assertEquals(ZPath.parse("/one/two/three"), ZPath.root.child("one").child("two").child("three"));
        assertEquals(ZPath.parse("/one/two/three"), ZPath.from("one", "two", "three"));
        assertEquals(ZPath.parseWithIds("/one/{id}/two/{id}"), ZPath.from("one", parameter(), "two", parameter()));
    }

    @Test
    public void testUnresolvedPath()
    {
        assertThrows(IllegalStateException.class, ()->{
            ZPath path = ZPath.from("one", parameter(), "two");
            path.fullPath();
        });
    }

    @Test
    public void testResolvedPath()
    {
        ZPath path = ZPath.from("one", parameter(), "two", parameter());
        assertEquals(path.resolved("a", "b"), ZPath.from("one", "a", "two", "b"));
    }

    @Test
    public void testSchema()
    {
        ZPath path = ZPath.from("one", parameter(), "two", parameter());
        assertEquals(path.toSchemaPathPattern().toString(), "/one/.*/two/.*");
        path = ZPath.parse("/one/two/three");
        assertEquals(path.toSchemaPathPattern().toString(), "/one/two/three");
        path = ZPath.parseWithIds("/one/{id}/three");
        assertEquals(path.toSchemaPathPattern().toString(), "/one/.*/three");
        path = ZPath.parseWithIds("/{id}/{id}/three");
        assertEquals(path.toSchemaPathPattern().toString(), "/.*/.*/three");
    }

    @Test
    public void testCustomIds()
    {
        assertEquals(ZPath.parseWithIds("/a/{a}/bee/{bee}/c/{c}").toString(), "/a/{a}/bee/{bee}/c/{c}");
        assertEquals(ZPath.from("a", parameter(), "b", parameter()).toString(), "/a/{id}/b/{id}");
        assertEquals(ZPath.from("a", parameter("foo"), "b", parameter("bar")).toString(), "/a/{foo}/b/{bar}");
    }

    @Test
    public void testPartialResolution()
    {
        ZPath path = ZPath.parseWithIds("/one/{1}/two/{2}");
        assertFalse(path.parent().isResolved());
        assertFalse(path.parent().parent().isResolved());
        assertTrue(path.parent().parent().parent().isResolved());
        assertFalse(path.isResolved());

        path = path.resolved("p1");
        assertFalse(path.isResolved());
        assertTrue(path.parent().isResolved());
        assertEquals(path.toString(), "/one/p1/two/{2}");

        path = path.resolved("p2");
        assertTrue(path.isResolved());
        assertEquals(path.toString(), "/one/p1/two/p2");
    }
}
