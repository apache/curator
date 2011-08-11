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
package com.netflix.curator.drivers;

public interface LoggingDriver
{
    /**
     * Logs the message at the DEBUG level
     *
     * @param message The message to be logged.
     */
    public void debug(String message);

    /**
     * Logs the throwable at the DEBUG level
     *
     * @param theThrowable The throwable to be logged.
     */
    public void debug(Throwable theThrowable);

    /**
     * Logs the throwable after prepending the given message at the DEBUG level
     *
     * @param message The message to be prepended to the throwable provided
     * @param theThrowable The throwable to be logged.
     */
    public void debug(String message, Throwable theThrowable);

    /**
     * Logs the message at the INFO level
     *
     * @param message The message to be logged.
     */
    public void info(String message);

    /**
     * Logs the throwable at the INFO level
     *
     * @param theThrowable The throwable to be logged.
     */
    public void info(Throwable theThrowable);

    /**
     * Logs the throwable after prepending the given message at the INFO level
     *
     * @param message The message to be prepended to the throwable provided
     * @param theThrowable The throwable to be logged.
     */
    public void info(String message, Throwable theThrowable);

    /**
     * Logs the message at the WARN level
     *
     * @param message The message to be logged.
     */
    public void warn(String message);

    /**
     * Logs the throwable at the WARN level
     *
     * @param theThrowable The throwable to be logged.
     */
    public void warn(Throwable theThrowable);

    /**
     * Logs the throwable after prepending the given message at the WARN level
     *
     * @param message The message to be prepended to the throwable provided
     * @param theThrowable The throwable to be logged.
     */
    public void warn(String message, Throwable theThrowable);

    /**
     * Logs the message at the ERROR level
     *
     * @param message The message to be logged.
     */
    public void error(String message);

    /**
     * Logs the throwable at the ERROR level
     *
     * @param theThrowable The throwable to be logged.
     */
    public void error(Throwable theThrowable);

    /**
     * Logs the throwable after prepending the given message at the ERROR level
     *
     * @param message The message to be prepended to the throwable provided
     * @param theThrowable The throwable to be logged.
     */
    public void error(String message, Throwable theThrowable);
}
