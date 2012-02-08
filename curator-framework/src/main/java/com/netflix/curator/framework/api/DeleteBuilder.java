/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.curator.framework.api;

import com.netflix.curator.framework.CuratorFramework;

public interface DeleteBuilder extends DeleteBuilderBase
{
    /**
     * <p>
     *     Solves this edge case: deleting a node can fail due to connection issues. Further,
     *     if the node was ephemeral, the node will not get auto-deleted as the session is still valid.
     *     This can wreak havoc with lock implementations.
     * </p>
     * 
     * <p>
     *     When <code>guaranteed</code> is set, Curator will record failed node deletions and 
     *     attempt to delete them in the background until successful. NOTE: you will still get an
     *     exception when the deletion fails. But, you can be assured that as long as the 
     *     {@link CuratorFramework} instance is open attempts will be made to delete the node.
     * </p>
     *  
     * @return this
     */
    public DeleteBuilderBase    guaranteed();
}
