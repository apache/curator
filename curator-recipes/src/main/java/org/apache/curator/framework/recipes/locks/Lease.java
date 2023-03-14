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

package org.apache.curator.framework.recipes.locks;

import java.io.Closeable;
import java.io.IOException;

/**
 * Represents an acquired lease from an {@link InterProcessSemaphore}. It is the client's responsibility
 * to close this lease when it is no longer needed so that other blocked clients can use it. If the
 * client crashes (or its session expires, etc.) the lease will automatically be closed.
 */
public interface Lease extends Closeable
{
    /**
     * Releases the lease so that other clients/processes can acquire it
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException;

    /**
     * Return the data stored in the node for this lease
     *
     * @return data
     * @throws Exception errors
     */
    public byte[]   getData() throws Exception;

    /**
     * Return the the node for this lease
     *
     * @return data
     * @throws Exception errors
     */
    public String getNodeName();
}
