/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.curator.framework.recipes.queue;

import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

class ItemSerializer
{
    private static final int    VERSION = 0x00010001;

    private static final byte   ITEM_OPCODE = 0x01;
    private static final byte   EOF_OPCODE = 0x02;

    private static final int    INITIAL_BUFFER_SIZE = 0x1000;

    static<T> MultiItem<T>  deserialize(byte[] bytes, QueueSerializer<T> serializer) throws Exception
    {
        DataInputStream     in = new DataInputStream(new ByteArrayInputStream(bytes));
        int                 version = in.readInt();
        if ( version != VERSION )
        {
            throw new IOException(String.format("Incorrect version. Expected %d - Found: %d", VERSION, version));
        }

        List<T>             items = Lists.newArrayList();
        for(;;)
        {
            byte    opcode = in.readByte();
            if ( opcode == EOF_OPCODE )
            {
                break;
            }

            if ( opcode != ITEM_OPCODE )
            {
                throw new IOException(String.format("Incorrect opcode. Expected %d - Found: %d", ITEM_OPCODE, opcode));
            }
            int     size = in.readInt();
            if ( size < 0 )
            {
                throw new IOException(String.format("Bad size: %d", size));
            }
            byte[]     itemBytes = new byte[size];
            if ( size > 0 )
            {
                in.readFully(itemBytes);
            }
            items.add(serializer.deserialize(itemBytes));
        }

        final Iterator<T>   iterator = items.iterator();
        return new MultiItem<T>()
        {
            @Override
            public T nextItem()
            {
                return iterator.hasNext() ? iterator.next() : null;
            }
        };
    }

    static<T> byte[]        serialize(MultiItem<T> items, QueueSerializer<T> serializer) throws Exception
    {
        ByteArrayOutputStream       bytes = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
        DataOutputStream            out = new DataOutputStream(bytes);
        out.writeInt(VERSION);
        for(;;)
        {
            T   item = items.nextItem();
            if ( item == null )
            {
                break;
            }
            byte[]      itemBytes = serializer.serialize(item);
            out.writeByte(ITEM_OPCODE);
            out.writeInt(itemBytes.length);
            if ( itemBytes.length > 0 )
            {
                out.write(itemBytes);
            }
        }
        out.writeByte(EOF_OPCODE);
        out.close();

        return bytes.toByteArray();
    }

    private ItemSerializer()
    {
    }
}
