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

package pfouto.messages.side;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import pfouto.Clock;
import pfouto.proxy.EngageProxy;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class DataMessage extends ProtoMessage
{
    public static final short MSG_ID = 101;

    private static final Logger logger = LoggerFactory.getLogger(DataMessage.class);

    public static ISerializer<DataMessage> serializer = new ISerializer<DataMessage>()
    {
        @Override
        public void serialize(DataMessage dataMessage, ByteBuf byteBuf) throws IOException
        {
            DataOutputBuffer buffer = new DataOutputBuffer();
            Mutation.serializer.serialize(dataMessage.mutation, buffer, MessagingService.VERSION_40);
            byte[] dataData = buffer.getData();
            byteBuf.writeInt(dataData.length);
            byteBuf.writeBytes(dataData);
            Clock.serializer.serialize(dataMessage.vectorClock, byteBuf);
            byteBuf.writeInt(dataMessage.vUp);
        }

        @Override
        public DataMessage deserialize(ByteBuf byteBuf) throws IOException
        {
            int dataSize = byteBuf.readInt();
            byte[] dataData = new byte[dataSize];
            byteBuf.readBytes(dataData);
            Mutation deserialize = Mutation.serializer.deserialize(new DataInputBuffer(dataData), MessagingService.VERSION_40);
            Clock clock = Clock.serializer.deserialize(byteBuf);
            int vUp = byteBuf.readInt();
            return new DataMessage(deserialize, clock, vUp);
        }
    };
    private final Mutation mutation;
    private final Clock vectorClock;
    private final int vUp;

    public DataMessage(Mutation mutation, Clock vectorClock, int vUp)
    {
        super(MSG_ID);
        this.mutation = mutation;
        this.vectorClock = vectorClock;
        this.vUp = vUp;
    }

    public int getvUp()
    {
        return vUp;
    }

    public Clock getVectorClock()
    {
        return vectorClock;
    }

    public Mutation getMutation()
    {
        return mutation;
    }

    @Override
    public String toString()
    {
        return "DataMessage{" +
               "mutation=" + mutation +
               ", vectorClock=" + vectorClock +
               ", vUp=" + vUp +
               '}';
    }
}
