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

import org.apache.zookeeper.CreateMode;

public interface CreateModable<T>
{
    /**
     * Set a create mode - the default is {@link CreateMode#PERSISTENT}
     *
     * @param mode new create mode
     * @return this
     */
    public default T withMode(CreateMode mode)
    {
        return withMode(mode, PathEncodingType.DEFAULT);
    }

    /**
     * Set a create mode with a path encoding option - the default is {@link CreateMode#PERSISTENT} and
     * {@link PathEncodingType#DEFAULT}.
     *
     * @param mode new create mode
     * @param pathEncodingType path encoding type
     * @return this
     */
    public T withMode(CreateMode mode, PathEncodingType pathEncodingType);
}
