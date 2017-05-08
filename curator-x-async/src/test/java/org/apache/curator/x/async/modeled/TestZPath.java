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

import static org.apache.curator.x.async.modeled.ZPath.parameterNodeName;

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
    }

    @Test
    public void testParsing()
    {
        Assert.assertEquals(ZPath.parse("/"), ZPath.root);
        Assert.assertEquals(ZPath.parse("/one/two/three"), ZPath.root.at("one").at("two").at("three"));
        Assert.assertEquals(ZPath.parse("/one/two/three"), ZPath.from("one", "two", "three"));
        Assert.assertEquals(ZPath.parseWithIds("/one/{id}/two/{id}"), ZPath.from("one", parameterNodeName, "two", parameterNodeName));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testUnresolvedPath()
    {
        ZPath path = ZPath.from("one", parameterNodeName, "two");
        path.fullPath();
    }

    @Test
    public void testResolvedPath()
    {
        ZPath path = ZPath.from("one", parameterNodeName, "two", parameterNodeName);
        Assert.assertEquals(path.resolved("a", "b"), ZPath.from("one", "a", "two", "b"));
    }

    @Test
    public void testSchema()
    {
        ZPath path = ZPath.from("one", parameterNodeName, "two", parameterNodeName);
        Assert.assertEquals(path.toSchemaPathPattern().toString(), "/one/.*/two/.*");
        path = ZPath.parse("/one/two/three");
        Assert.assertEquals(path.toSchemaPathPattern().toString(), "/one/two/three");
        path = ZPath.parseWithIds("/one/{id}/three");
        Assert.assertEquals(path.toSchemaPathPattern().toString(), "/one/.*/three");
        path = ZPath.parseWithIds("/{id}/{id}/three");
        Assert.assertEquals(path.toSchemaPathPattern().toString(), "/.*/.*/three");
    }
}
