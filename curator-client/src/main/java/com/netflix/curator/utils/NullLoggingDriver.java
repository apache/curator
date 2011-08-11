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
package com.netflix.curator.utils;

import com.netflix.curator.drivers.LoggingDriver;
import com.netflix.curator.drivers.LoggingDriver;

/**
 * Default logging driver that does nothing
 */
public class NullLoggingDriver implements LoggingDriver
{
    @Override
    public void debug(String message)
    {
    }

    @Override
    public void debug(Throwable theThrowable)
    {
    }

    @Override
    public void debug(String message, Throwable theThrowable)
    {
    }

    @Override
    public void info(String message)
    {
    }

    @Override
    public void info(Throwable theThrowable)
    {
    }

    @Override
    public void info(String message, Throwable theThrowable)
    {
    }

    @Override
    public void warn(String message)
    {
    }

    @Override
    public void warn(Throwable theThrowable)
    {
    }

    @Override
    public void warn(String message, Throwable theThrowable)
    {
    }

    @Override
    public void error(String message)
    {
    }

    @Override
    public void error(Throwable theThrowable)
    {
    }

    @Override
    public void error(String message, Throwable theThrowable)
    {
    }
}
