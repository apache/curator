/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.curator.framework.imps;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.test.BaseClassForTests;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

public class TestCombinations extends BaseClassForTests
{
    @Test
    public void testCreate() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.builder().build();
        client.start();

        try {
            // Don't care what this does, just that it compiles and makes sense
            client.create()
            .creatingParentsIfNeeded()
            .withProtection()
            .compressed()
            .withMode(CreateMode.EPHEMERAL)
            .withACL(Collections.singletonList(new ACL()))
            .inBackground()
            .forPath("/");
        } catch (Exception e) {
        }
    }

    @Test
    public void testDelete() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.builder().build();
        client.start();

        try {
            // Don't care what this does, just that it compiles.
            client.delete()
            .guaranteed()
            .deletingChildrenIfNeeded()
            .withVersion(0)
            .inBackground()
            .forPath("/");
        } catch (Exception e) {
        }
    }

    @Test
    public void testSetData() throws Exception
    {
        CuratorFramework client = CuratorFrameworkFactory.builder().build();
        client.start();

        try {
            // Don't care what this does, just that it compiles.
            client.setData()
            .compressed()
            .withVersion(0)
            .inBackground()
            .forPath("/");
        } catch (Exception e) {
        }
    }
}
