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

package org.apache.curator.framework.api;

import org.apache.zookeeper.Watcher;

/**
 * Builder to allow watches to be removed 
 */
public interface RemoveWatchesBuilder
{
    /**
     * Specify the watcher to be removed
     * @param watcher
     * @return
     */
    public RemoveWatchesType remove(Watcher watcher);
    
    /**
     * Specify the watcher to be removed
     * @param watcher
     * @return
     */
    public RemoveWatchesType remove(CuratorWatcher watcher);
    
    /**
     * Specify that all watches should be removed
     * @return
     */
    public RemoveWatchesType removeAll();
}
