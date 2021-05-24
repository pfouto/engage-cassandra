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

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import pfouto.Clock;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class DataMessage extends ProtoMessage
{
    public static final short MSG_ID = 101;

    public static ISerializer<DataMessage> serializer = new ISerializer<DataMessage>()
    {
        @Override
        public void serialize(DataMessage dataMessage, ByteBuf byteBuf) throws IOException
        {
            Mutation.serializer.serialize(dataMessage.mutation, new DataOutputBuffer(byteBuf.nioBuffer()), MessagingService.VERSION_40);
            Clock.serializer.serialize(dataMessage.vectorClock, byteBuf);
            byteBuf.writeInt(dataMessage.vUp);
        }

        @Override
        public DataMessage deserialize(ByteBuf byteBuf) throws IOException
        {
            Mutation deserialize = Mutation.serializer.deserialize(new DataInputBuffer(byteBuf.nioBuffer(), false), MessagingService.VERSION_40);
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
