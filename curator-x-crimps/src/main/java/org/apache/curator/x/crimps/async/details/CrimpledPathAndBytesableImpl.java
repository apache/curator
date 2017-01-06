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
package org.apache.curator.x.crimps.async.details;

import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.api.Pathable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class CrimpledPathAndBytesableImpl<MAIN, RESULT extends CompletionStage<MAIN>> implements CrimpledPathAndBytesable<RESULT>
{
    private final Pathable<MAIN> pathable;
    private final PathAndBytesable<MAIN> pathAndBytesable;
    private final RESULT result;
    private final CompletableFuture second;

    CrimpledPathAndBytesableImpl(Pathable<MAIN> pathable, RESULT result, CompletableFuture second)
    {
        this.result = result;
        this.second = second;
        this.pathAndBytesable = null;
        this.pathable = pathable;
    }

    CrimpledPathAndBytesableImpl(PathAndBytesable<MAIN> pathAndBytesable, RESULT result, CompletableFuture second)
    {
        this.pathAndBytesable = pathAndBytesable;
        this.result = result;
        this.second = second;
        this.pathable = null;
    }

    @Override
    public RESULT forPath(String path)
    {
        try
        {
            if ( pathable != null )
            {
                pathable.forPath(path);
            }
            else
            {
                pathAndBytesable.forPath(path);
            }
        }
        catch ( Exception e )
        {
            setException(e);
        }
        return result;
    }

    @Override
    public RESULT forPath(String path, byte[] data)
    {
        try
        {
            pathAndBytesable.forPath(path, data);
        }
        catch ( Exception e )
        {
            setException(e);
        }
        return result;
    }

    private void setException(Exception e)
    {
        result.toCompletableFuture().completeExceptionally(e);
        if ( second != null )
        {
            second.completeExceptionally(e);
        }
    }
}
