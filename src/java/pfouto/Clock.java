/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pfouto;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

public class Clock
{

    public static ISerializer<Clock> serializer = new ISerializer<>()
    {
        @Override
        public void serialize(Clock clock, ByteBuf byteBuf) throws IOException
        {
            byteBuf.writeInt(clock.value.size());
            clock.value.forEach((k, v) -> {
                byteBuf.writeBytes(k.getAddress());
                byteBuf.writeInt(v);
            });
        }

        @Override
        public Clock deserialize(ByteBuf byteBuf) throws IOException
        {
            Map<Inet4Address, Integer> map = new HashMap<>();
            int size = byteBuf.readInt();
            for (int i = 0; i < size; i++)
            {
                byte[] addrBytes = new byte[4];
                byteBuf.readBytes(addrBytes);
                Inet4Address addr = (Inet4Address) Inet4Address.getByAddress(addrBytes);
                int val = byteBuf.readInt();
                map.put(addr, val);
            }
            return new Clock(map);
        }
    };

    private final Map<Inet4Address, Integer> value;

    public Clock(Map<Inet4Address, Integer> value)
    {
        this.value = value;
    }

    public Clock()
    {
        this.value = new HashMap<>();
    }

    public static Clock fromInputStream(InputStream data) throws IOException
    {
        Map<Inet4Address, Integer> map = new HashMap<>();
        try (DataInputStream dis = new DataInputStream(data))
        {
            int size = dis.readInt();
            for (int i = 0; i < size; i++)
            {
                Inet4Address addr = (Inet4Address) Inet4Address.getByAddress(dis.readNBytes(4));
                int val = dis.readInt();
                map.put(addr, val);
            }
        }
        return new Clock(map);
    }

    public void merge(Inet4Address key, Integer val)
    {
        value.merge(key, val, Math::max);
    }

    public void merge(Clock other)
    {
        other.value.forEach(this::merge);
    }

    public ByteBuffer toBuffer()
    {
        ByteBuffer allocate = ByteBuffer.allocate((value.size() * 2 + 1) * 4);
        allocate.putInt(value.size());
        value.forEach((k, v) -> {
            allocate.put(k.getAddress());
            allocate.putInt(v);
        });
        return allocate;
    }

    public Map<Inet4Address, Integer> getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return value.toString();
    }
}
