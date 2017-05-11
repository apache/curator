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

import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.async.modeled.details.ZPathImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.curator.x.async.modeled.ZPath.parameter;

public class TestZPath
{
    @Test
    public void testRoot()
    {
        Assert.assertEquals(ZPath.root.nodeName(), ZKPaths.PATH_SEPARATOR);
        Assert.assertEquals(ZPath.root, ZPathImpl.root);
        Assert.assertTrue(ZPath.root.isRoot());
        Assert.assertEquals(ZPath.root.at("foo").parent(), ZPath.root);
        Assert.assertTrue(ZPath.root.at("foo").parent().isRoot());
    }

    @Test
    public void testBasic()
    {
        ZPath path = ZPath.root.at("one").at("two");
        Assert.assertFalse(path.isRoot());
        Assert.assertEquals(path, ZPath.root.at("one").at("two"));
        Assert.assertNotEquals(path, ZPath.root.at("onex").at("two"));
        Assert.assertEquals(path.nodeName(), "two");
        Assert.assertEquals(path.fullPath(), "/one/two");
        Assert.assertEquals(path.parent().fullPath(), "/one");
        Assert.assertEquals(path.fullPath(), "/one/two");       // call twice to test the internal cache
        Assert.assertEquals(path.parent().fullPath(), "/one");  // call twice to test the internal cache

        Assert.assertTrue(path.startsWith(ZPath.root.at("one")));
        Assert.assertFalse(path.startsWith(ZPath.root.at("two")));

        ZPath checkIdLike = ZPath.parse("/one/{two}/three");
        Assert.assertTrue(checkIdLike.isResolved());
        checkIdLike = ZPath.parse("/one/" + ZPath.parameter() + "/three");
        Assert.assertTrue(checkIdLike.isResolved());
        checkIdLike = ZPath.parse("/one/" + ZPath.parameter("others") + "/three");
        Assert.assertTrue(checkIdLike.isResolved());
    }

    @Test
    public void testParsing()
    {
        Assert.assertEquals(ZPath.parse("/"), ZPath.root);
        Assert.assertEquals(ZPath.parse("/one/two/three"), ZPath.root.at("one").at("two").at("three"));
        Assert.assertEquals(ZPath.parse("/one/two/three"), ZPath.from("one", "two", "three"));
        Assert.assertEquals(ZPath.parseWithIds("/one/{id}/two/{id}"), ZPath.from("one", parameter(), "two", parameter()));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testUnresolvedPath()
    {
        ZPath path = ZPath.from("one", parameter(), "two");
        path.fullPath();
    }

    @Test
    public void testResolvedPath()
    {
        ZPath path = ZPath.from("one", parameter(), "two", parameter());
        Assert.assertEquals(path.resolved("a", "b"), ZPath.from("one", "a", "two", "b"));
    }

    @Test
    public void testSchema()
    {
        ZPath path = ZPath.from("one", parameter(), "two", parameter());
        Assert.assertEquals(path.toSchemaPathPattern().toString(), "/one/.*/two/.*");
        path = ZPath.parse("/one/two/three");
        Assert.assertEquals(path.toSchemaPathPattern().toString(), "/one/two/three");
        path = ZPath.parseWithIds("/one/{id}/three");
        Assert.assertEquals(path.toSchemaPathPattern().toString(), "/one/.*/three");
        path = ZPath.parseWithIds("/{id}/{id}/three");
        Assert.assertEquals(path.toSchemaPathPattern().toString(), "/.*/.*/three");
    }

    @Test
    public void testCustomIds()
    {
        Assert.assertEquals(ZPath.parseWithIds("/a/{a}/bee/{bee}/c/{c}").toString(), "/a/{a}/bee/{bee}/c/{c}");
        Assert.assertEquals(ZPath.from("a", parameter(), "b", parameter()).toString(), "/a/{id}/b/{id}");
        Assert.assertEquals(ZPath.from("a", parameter("foo"), "b", parameter("bar")).toString(), "/a/{foo}/b/{bar}");
    }

    @Test
    public void testPartialResolution()
    {
        ZPath path = ZPath.parseWithIds("/one/{1}/two/{2}");
        Assert.assertFalse(path.parent().isResolved());
        Assert.assertFalse(path.parent().parent().isResolved());
        Assert.assertTrue(path.parent().parent().parent().isResolved());
        Assert.assertFalse(path.isResolved());

        path = path.resolved("p1");
        Assert.assertFalse(path.isResolved());
        Assert.assertTrue(path.parent().isResolved());
        Assert.assertEquals(path.toString(), "/one/p1/two/{2}");

        path = path.resolved("p2");
        Assert.assertTrue(path.isResolved());
        Assert.assertEquals(path.toString(), "/one/p1/two/p2");
    }
}
