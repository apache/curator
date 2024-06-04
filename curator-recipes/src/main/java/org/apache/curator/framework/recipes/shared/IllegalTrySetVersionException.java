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

package org.apache.curator.framework.recipes.shared;

import org.apache.zookeeper.data.Stat;

/**
 * Exception to alert overflowed {@link Stat#getVersion()} {@code -1} which is not suitable in
 * {@link SharedValue#trySetValue(VersionedValue, byte[])} and {@link SharedCount#trySetCount(VersionedValue, int)}.
 *
 * <p>In case of this exception, clients have to choose:
 * <ul>
 *     <li>Take their own risk to do a blind set.</li>
 *     <li>Update ZooKeeper cluster to solve <a href="https://issues.apache.org/jira/browse/ZOOKEEPER-4743">ZOOKEEPER-4743</a>.</li>
 * </ul>
 */
public class IllegalTrySetVersionException extends IllegalArgumentException {
    @Override
    public String getMessage() {
        return "overflowed Stat.version -1 is not suitable for trySet(a.k.a. compare-and-set ZooKeeper::setData)";
    }
}
