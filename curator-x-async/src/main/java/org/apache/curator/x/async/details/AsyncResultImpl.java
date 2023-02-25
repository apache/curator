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

package org.apache.curator.x.async.details;

import org.apache.curator.x.async.AsyncResult;
import org.apache.zookeeper.KeeperException;
import java.util.Objects;
import java.util.Optional;

public class AsyncResultImpl<T> implements AsyncResult<T>
{
    private final T value;
    private final KeeperException.Code code;
    private final Throwable exception;

    public AsyncResultImpl()
    {
        this(null, KeeperException.Code.OK, null);
    }

    public AsyncResultImpl(KeeperException.Code code)
    {
        this(null, code, null);
    }

    public AsyncResultImpl(T value)
    {
        this(value, KeeperException.Code.OK, null);
    }

    public AsyncResultImpl(Throwable exception)
    {
        this(null, KeeperException.Code.SYSTEMERROR, exception);
    }

    private AsyncResultImpl(T value, KeeperException.Code code, Throwable exception)
    {
        this.value = value;
        this.exception = exception;
        this.code = Objects.requireNonNull(code, "error cannot be null");
    }

    public T getRawValue()
    {
        return value;
    }

    public Optional<T> getValue()
    {
        return Optional.ofNullable(value);
    }

    public KeeperException.Code getCode()
    {
        return code;
    }

    public Throwable getRawException()
    {
        return exception;
    }

    public Optional<Throwable> getException()
    {
        return Optional.ofNullable(exception);
    }

    public void checkException()
    {
        if ( exception != null )
        {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void checkError()
    {
        checkException();
        if ( code != KeeperException.Code.OK )
        {
            throw new RuntimeException(KeeperException.create(code));
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        AsyncResultImpl<?> that = (AsyncResultImpl<?>)o;

        if ( value != null ? !value.equals(that.value) : that.value != null )
        {
            return false;
        }
        //noinspection SimplifiableIfStatement
        if ( code != that.code )
        {
            return false;
        }
        return exception != null ? exception.equals(that.exception) : that.exception == null;
    }

    @Override
    public int hashCode()
    {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + code.hashCode();
        result = 31 * result + (exception != null ? exception.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "AsyncResult{" + "value=" + value + ", code=" + code + ", exception=" + exception + '}';
    }
}
