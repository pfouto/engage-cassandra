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

package pfouto.messages.up;

import java.io.IOException;
import java.net.InetAddress;

import io.netty.buffer.ByteBuf;
import pfouto.Clock;
import pfouto.Utils;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class MetadataFlush extends ProtoMessage
{
    public static final short MSG_ID = 203;
    public static final ISerializer<MetadataFlush> serializer = new ISerializer<MetadataFlush>()
    {
        @Override
        public void serialize(MetadataFlush msg, ByteBuf out) throws IOException
        {
            out.writeBytes(msg.source.getAddress());
            out.writeInt(msg.vUp);
        }

        @Override
        public MetadataFlush deserialize(ByteBuf in) throws IOException
        {
            byte[] addrBytes = new byte[4];
            in.readBytes(addrBytes);
            int vUp = in.readInt();
            return new MetadataFlush(InetAddress.getByAddress(addrBytes), vUp);
        }
    };
    private final InetAddress source;
    private final int vUp;

    public MetadataFlush(InetAddress source, int vUp)
    {
        super(MSG_ID);
        this.source = source;
        this.vUp = vUp;
    }

    public InetAddress getSource()
    {
        return source;
    }

    public int getvUp()
    {
        return vUp;
    }

}
