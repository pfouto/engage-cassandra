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
import java.net.Inet4Address;
import java.net.InetAddress;

import io.netty.buffer.ByteBuf;
import jnr.ffi.annotations.Meta;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import pfouto.Clock;
import pfouto.Utils;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class UpdateNotification extends ProtoMessage
{
    public static final short MSG_ID = 202;
    public static final ISerializer<UpdateNotification> serializer = new ISerializer<UpdateNotification>()
    {
        @Override
        public void serialize(UpdateNotification msg, ByteBuf out) throws IOException
        {
            out.writeBytes(msg.source.getAddress());
            out.writeInt(msg.vUp);
            Utils.serializeString(msg.partition, out);
            Clock.serializer.serialize(msg.vectorClock, out);
            if(msg.data != null){
                out.writeInt(msg.data.length);
                out.writeBytes(msg.data);
            } else {
                out.writeInt(0);
            }
            if(msg.mf != null){
                out.writeBoolean(true);
                MetadataFlush.serializer.serialize(msg.mf, out);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public UpdateNotification deserialize(ByteBuf in) throws IOException
        {
            byte[] addrBytes = new byte[4];
            in.readBytes(addrBytes);
            int vUp = in.readInt();
            String partition = Utils.deserializeString(in);
            Clock clock = Clock.serializer.deserialize(in);
            int dataSize = in.readInt();
            byte[] data;
            if(dataSize > 0) {
                data = new byte[dataSize];
                in.readBytes(data);
            } else data = null;
            MetadataFlush mf;
            boolean mfPresent = in.readBoolean();
            if(mfPresent) mf = MetadataFlush.serializer.deserialize(in);
            else mf = null;
            return new UpdateNotification(InetAddress.getByAddress(addrBytes), vUp, partition, clock, data, mf);
        }
    };
    private final InetAddress source;
    private final int vUp;
    private final String partition;
    private final Clock vectorClock;
    private final byte[] data;
    private final MetadataFlush mf;

    public UpdateNotification(InetAddress source, int vUp, String partition, Clock vectorClock, byte[] data, MetadataFlush mf)
    {
        super(MSG_ID);
        this.source = source;
        this.vUp = vUp;
        this.partition = partition;
        this.vectorClock = vectorClock;
        this.data = data;
        this.mf = mf;
    }

    public byte[] getData()
    {
        return data;
    }

    public Clock getVectorClock()
    {
        return vectorClock;
    }

    public InetAddress getSource()
    {
        return source;
    }

    public int getvUp()
    {
        return vUp;
    }

    public String getPartition()
    {
        return partition;
    }

    public MetadataFlush getMf()
    {
        return mf;
    }
}
