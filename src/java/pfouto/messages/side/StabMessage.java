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

public class StabMessage extends ProtoMessage
{
    public static final short MSG_ID = 102;

    public static ISerializer<StabMessage> serializer = new ISerializer<StabMessage>()
    {
        @Override
        public void serialize(StabMessage dataMessage, ByteBuf byteBuf) throws IOException
        {
            byteBuf.writeInt(dataMessage.vUp);
        }

        @Override
        public StabMessage deserialize(ByteBuf byteBuf) throws IOException
        {
            int vUp = byteBuf.readInt();
            return new StabMessage(vUp);
        }
    };
    private final int vUp;

    public StabMessage(int vUp)
    {
        super(MSG_ID);
        this.vUp = vUp;
    }

    public int getvUp()
    {
        return vUp;
    }

    @Override
    public String toString()
    {
        return "StabMessage{" +
               "vUp=" + vUp +
               '}';
    }
}
