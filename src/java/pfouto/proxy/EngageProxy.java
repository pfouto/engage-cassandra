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

package pfouto.proxy;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import pfouto.Clock;
import pfouto.messages.side.DataMessage;
import pfouto.messages.side.StabMessage;
import pfouto.messages.up.MetadataFlush;
import pfouto.messages.up.UpdateNotification;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class EngageProxy extends GenericProxy
{
    private static final Logger logger = LoggerFactory.getLogger(EngageProxy.class);

    Set<Host> peers = new HashSet<>();

    Map<InetAddress, Queue<ProtoMessage>> pendingMetadata;
    Map<InetAddress, Map<Integer, ProtoMessage>> pendingData;

    public EngageProxy()
    {
        super("EngageProxy");
    }

    @Override
    void onDataMessage(DataMessage msg, Host host, short sourceProto, int channelId)
    {
        //TODO If queue empty, try execute - if fail, add to queue. if not empty, add to queue


        //TODO execute on mutation stage, and then send a notification back here to increment vector clock and try to
        //execute more operations

    }

    @Override
    void onMetadataFlush(MetadataFlush msg, Host host, short sourceProto, int channelId)
    {

    }

    @Override
    void onUpdateNotification(UpdateNotification msg, Host host, short sourceProto, int channelId)
    {
        onMetadataFlush(msg.getMf(), host, sourceProto, channelId);

    }

    @Override
    void onStabMessage(StabMessage msg, Host from, short sourceProto, int channelId)
    {
        throw new AssertionError("Unexpected message " + msg + " from " + from);
    }

    @Override
    void createConnections()
    {
        for (Map.Entry<String, List<Host>> entry : targets.entrySet())
            for (Host h : entry.getValue())
                if (peers.add(h))
                    openConnection(h, peerChannel);
    }

    //Called by Cassandra query executor thread pool. Not by babel threads!
    //Synchronized in StorageProxy
    @Override
    public void ship(Mutation mutation, Clock objectClock, int vUp)
    {
        //mutation = Mutation.serializer.deserialize(new DataInputBuffer(byteBuf.nioBuffer(), false), MessagingService.VERSION_40);
        //DataOutputBuffer buffer = new DataOutputBuffer();
        //Mutation.serializer.serialize(mutation, buffer, MessagingService.VERSION_40);

        try
        {
            String partition = mutation.getKeyspaceName();
            UpdateNotification not = new UpdateNotification(GenericProxy.myAddr, vUp, partition, objectClock, null, null);
            sendMessage(not, null, clientChannel);
            DataMessage dataMessage = new DataMessage(mutation, objectClock, vUp);
            targets.get(partition).forEach(h -> sendMessage(dataMessage, h));
        } catch(Exception e){
            logger.error("Exception in ship: " + e.getMessage());
            throw new AssertionError(e);
        }
    }
}
