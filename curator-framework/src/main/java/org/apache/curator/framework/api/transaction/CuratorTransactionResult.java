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

package org.apache.curator.framework.api.transaction;

import com.google.common.base.Predicate;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.data.Stat;

/**
 * Holds the result of one transactional operation
 */
public class CuratorTransactionResult
{
    private final OperationType type;
    private final String forPath;
    private final String resultPath;
    private final Stat resultStat;
    private final int error;

    /**
     * Utility that can be passed to Google Guava to find a particular result. E.g.
     * <pre>
     * Iterables.find(results, CuratorTransactionResult.ofTypeAndPath(OperationType.CREATE, path))
     * </pre>
     *
     * @param type operation type
     * @param forPath path
     * @return predicate
     */
    public static Predicate<CuratorTransactionResult> ofTypeAndPath(final OperationType type, final String forPath)
    {
        return new Predicate<CuratorTransactionResult>()
        {
            @Override
            public boolean apply(CuratorTransactionResult result)
            {
                return (result.getType() == type) && result.getForPath().equals(forPath);
            }
        };
    }

    public CuratorTransactionResult(OperationType type, String forPath, String resultPath, Stat resultStat)
    {
        this(type, forPath, resultPath, resultStat, 0);
    }

    public CuratorTransactionResult(OperationType type, String forPath, String resultPath, Stat resultStat, int error)
    {
        this.forPath = forPath;
        this.resultPath = resultPath;
        this.resultStat = resultStat;
        this.type = type;
        this.error = error;
    }

    /**
     * Returns the operation type
     *
     * @return operation type
     */
    public OperationType getType()
    {
        return type;
    }

    /**
     * Returns the path that was passed to the operation when added
     *
     * @return operation input path
     */
    public String getForPath()
    {
        return forPath;
    }

    /**
     * Returns the operation generated path or <code>null</code>. i.e. {@link CuratorTransaction#create()}
     * using an EPHEMERAL mode generates the created path plus its sequence number.
     *
     * @return generated path or null
     */
    public String getResultPath()
    {
        return resultPath;
    }

    /**
     * Returns the operation generated stat or <code>null</code>. i.e. {@link CuratorTransaction#setData()}
     * generates a stat object.
     *
     * @return generated stat or null
     */
    public Stat getResultStat()
    {
        return resultStat;
    }

    /**
     * Returns the operation generated error or <code>0</code> i.e. {@link OpResult.ErrorResult#getErr()}
     *
     * @return error or 0
     */
    public int getError()
    {
        return error;
    }
}
