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

package org.apache.curator.framework;

import org.apache.curator.framework.api.TempGetDataBuilder;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import java.io.Closeable;

/**
 * <p>
 *     Temporary CuratorFramework instances are meant for single requests to ZooKeeper ensembles
 *     over a failure prone network such as a WAN. The APIs available from CuratorTempFramework
 *     are limited. Further, the connection will be closed after a period of inactivity.
 * </p>
 *
 * <p>
 *     Based on an idea mentioned in a post by Camille Fournier:
 *     <a href="http://whilefalse.blogspot.com/2012/12/building-global-highly-available.html">http://whilefalse.blogspot.com/2012/12/building-global-highly-available.html</a>
 * </p>
 */
public interface CuratorTempFramework extends Closeable
{
    /**
     * Stop the client
     */
    public void     close();

    /**
     * Start a transaction builder
     *
     * @return builder object
     * @throws Exception errors
     */
    public CuratorTransaction inTransaction() throws Exception;

    /**
     * Start a get data builder
     *
     * @return builder object
     * @throws Exception errors
     */
    public TempGetDataBuilder getData() throws Exception;
}
