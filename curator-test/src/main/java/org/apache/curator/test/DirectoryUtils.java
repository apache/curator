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

package org.apache.curator.test;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// copied from Google Guava as these methods are now deprecated
// NOTE: removed the line of code documented: Symbolic links will have different canonical and absolute paths
// Update May 28, 2017 - change exception into logs
public class DirectoryUtils {
    private static final Logger log = LoggerFactory.getLogger(DirectoryUtils.class);

    public static File createTempDirectory() {
        try {
            final Path tempDirectory = Files.createTempDirectory(DirectoryUtils.class.getSimpleName());
            return tempDirectory.toFile();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void deleteRecursively(File file) throws IOException {
        if (file.isDirectory()) {
            deleteDirectoryContents(file);
        }
        if (!file.delete()) {
            log.error("Failed to delete " + file);
        }
    }

    public static void deleteDirectoryContents(File directory) throws IOException {
        Preconditions.checkArgument(directory.isDirectory(), "Not a directory: %s", directory);
        File[] files = directory.listFiles();
        if (files == null) {
            log.warn("directory.listFiles() returned null for: " + directory);
            return;
        }
        for (File file : files) {
            deleteRecursively(file);
        }
    }
}
