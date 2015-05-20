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
package org.apache.curator.framework.api;

import org.apache.zookeeper.Watcher.WatcherType;

/**
 * Builder to allow the specification of whether it is acceptable to remove client side watch information
 * in the case where ZK cannot be contacted. 
 */
public interface RemoveWatchesType extends RemoveWatchesLocal, Guaranteeable<BackgroundPathableQuietly<Void>>
{
   
    /**
     * Specify the type of watcher to be removed.
     * @param watcherType
     * @return
     */
    public RemoveWatchesLocal ofType(WatcherType watcherType);
    
}
