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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A logging driver that use standard JDK logging
 */
public class DefaultLoggingDriver implements LoggingDriver
{
    private final Logger log;

    public DefaultLoggingDriver()
    {
        log = Logger.getLogger("curator");
    }

    @Override
    public void debug(String message)
    {
        log.log(Level.FINE, message);
    }

    @Override
    public void debug(Throwable e)
    {
        log.log(Level.FINE, "", e);
    }

    @Override
    public void debug(String message, Throwable e)
    {
        log.log(Level.FINE, message, e);
    }

    @Override
    public void info(String message)
    {
        log.log(Level.INFO, message);
    }

    @Override
    public void info(Throwable e)
    {
        log.log(Level.INFO, "", e);
    }

    @Override
    public void info(String message, Throwable e)
    {
        log.log(Level.INFO, message, e);
    }

    @Override
    public void warn(String message)
    {
        log.log(Level.WARNING, message);
    }

    @Override
    public void warn(Throwable e)
    {
        log.log(Level.WARNING, "", e);
    }

    @Override
    public void warn(String message, Throwable e)
    {
        log.log(Level.WARNING, message, e);
    }

    @Override
    public void error(String message)
    {
        log.log(Level.SEVERE, message);
    }

    @Override
    public void error(Throwable e)
    {
        log.log(Level.SEVERE, "", e);
    }

    @Override
    public void error(String message, Throwable e)
    {
        log.log(Level.SEVERE, message, e);
    }
}
