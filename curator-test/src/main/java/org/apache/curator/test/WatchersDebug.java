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

package org.apache.curator.test;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.zookeeper.ZooKeeper;

public class WatchersDebug {
    private static final Method getDataWatches;
    private static final Method getExistWatches;
    private static final Method getChildWatches;

    static {
        Method localGetDataWatches = null;
        Method localGetExistWatches = null;
        Method localGetChildWatches = null;
        try {
            localGetDataWatches = getMethod("getDataWatches");
            localGetExistWatches = getMethod("getExistWatches");
            localGetChildWatches = getMethod("getChildWatches");
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        getDataWatches = localGetDataWatches;
        getExistWatches = localGetExistWatches;
        getChildWatches = localGetChildWatches;
    }

    public static List<String> getDataWatches(ZooKeeper zooKeeper) {
        return callMethod(zooKeeper, WatchersDebug.getDataWatches);
    }

    public static List<String> getExistWatches(ZooKeeper zooKeeper) {
        return callMethod(zooKeeper, getExistWatches);
    }

    public static List<String> getChildWatches(ZooKeeper zooKeeper) {
        return callMethod(zooKeeper, getChildWatches);
    }

    private WatchersDebug() {}

    private static Method getMethod(String name) throws NoSuchMethodException {
        Method m = ZooKeeper.class.getDeclaredMethod(name);
        m.setAccessible(true);
        return m;
    }

    private static List<String> callMethod(ZooKeeper zooKeeper, Method method) {
        if (zooKeeper == null) {
            return null;
        }
        try {
            //noinspection unchecked
            return (List<String>) method.invoke(zooKeeper);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
