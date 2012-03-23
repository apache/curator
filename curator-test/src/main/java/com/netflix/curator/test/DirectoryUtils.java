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

package com.netflix.curator.test;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;

// copied from Google Guava as these methods are now deprecated
// NOTE: removed the line of code documented: Symbolic links will have different canonical and absolute paths
public class DirectoryUtils
{
    public static void deleteRecursively(File file) throws IOException
    {
        if (file.isDirectory()) {
            deleteDirectoryContents(file);
        }
        if (!file.delete()) {
            throw new IOException("Failed to delete " + file);
        }
    }

    public static void deleteDirectoryContents(File directory)
        throws IOException {
        Preconditions.checkArgument(directory.isDirectory(),
            "Not a directory: %s", directory);
        File[] files = directory.listFiles();
        if (files == null) {
            throw new IOException("Error listing files for " + directory);
        }
        for (File file : files) {
            deleteRecursively(file);
        }
    }
}
