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

package org.apache.curator.utils;

public class DebugUtils
{
    public static final String PROPERTY_LOG_EVENTS = "curator-log-events";
    public static final String PROPERTY_DONT_LOG_CONNECTION_ISSUES = "curator-dont-log-connection-problems";
    public static final String PROPERTY_LOG_ONLY_FIRST_CONNECTION_ISSUE_AS_ERROR_LEVEL = "curator-log-only-first-connection-issue-as-error-level";
    public static final String PROPERTY_REMOVE_WATCHERS_IN_FOREGROUND = "curator-remove-watchers-in-foreground";
    public static final String PROPERTY_RETRY_FAILED_TESTS = "curator-retry-failed-tests";
    public static final String PROPERTY_CHECK_INJECTED_DEBUG_EXCEPTIONS = "curator-check-injected-debug-exceptions";

    private DebugUtils()
    {
    }
}
