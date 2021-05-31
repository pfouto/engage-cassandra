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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.StorageProxy;
import pfouto.Clock;
import pfouto.MutableInteger;
import pfouto.ipc.MutationFinished;
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
    Map<InetAddress, Map<Integer, DataMessage>> pendingData;
    Map<InetAddress, Integer> executingData;

    public EngageProxy()
    {
        super("EngageProxy");
    }

    private boolean canExec(Clock opClock)
    {
        for (Map.Entry<Inet4Address, Integer> opClockPos : opClock.getValue().entrySet())
            if (StorageProxy.globalClock.computeIfAbsent(opClockPos.getKey(), k -> new MutableInteger()).getValue()
                < opClockPos.getValue())
                return false;
        return true;
    }

    private void startExec(DataMessage msg, InetAddress source)
    {
        if (msg.getvUp() != executingData.computeIfAbsent(source, k-> 0) + 1){
            logger.error("Executing unexpected op {} {}", msg.getvUp(), executingData.get(source));
            throw new AssertionError();
        }
        executingData.put(source, executingData.get(source)+1);
        Stage.MUTATION.maybeExecuteImmediately(() -> { //This block will run on mutation threadpool
            try
            {
                msg.getMutation().apply();
                sendRequest(new MutationFinished(msg, source), this.getProtoId());
            }
            catch (Exception e)
            {
                logger.error("Failed to apply remote mutation locally : ", e);
                throw new AssertionError(e);
            }
        });
    }

    @Override
    void onMutationFinished(MutationFinished request, short sourceProto)
    {
        InetAddress source = request.getSource();
        if(request.getMutationMessage().getvUp() != executingData.get(source) ||
           request.getMutationMessage().getvUp() !=
           StorageProxy.globalClock.computeIfAbsent(source, k-> new MutableInteger()).getValue()){
            logger.error("Finished executed unexpected op {} {} {}", request.getMutationMessage().getvUp(),
                         executingData.get(source), StorageProxy.globalClock.get(source));
            throw new AssertionError();
        }

        int highestClock = request.getMutationMessage().getvUp(); //Store value to set in highestClock

        //Exec following MF in this queue
        Queue<ProtoMessage> protoMessages = pendingMetadata.get(source);
        while(protoMessages.peek() instanceof MetadataFlush){
            MetadataFlush mfMsg = (MetadataFlush) protoMessages.remove();
            highestClock = mfMsg.getUpdates().get(source);
        }

        //Update clock, notifying waiting ops
        MutableInteger cPos = StorageProxy.globalClock.computeIfAbsent(source, k-> new MutableInteger());
        synchronized (cPos){
            cPos.setValue(highestClock);
            cPos.notifyAll();
        }
        executingData.put(source, highestClock);

        //Check if can exec anything else
        for (Map.Entry<InetAddress, Queue<ProtoMessage>> entry : pendingMetadata.entrySet())
        {
            InetAddress host = entry.getKey();
            ProtoMessage nextMsg = entry.getValue().peek();
            if(nextMsg instanceof UpdateNotification) {
                if(canExec(((UpdateNotification) nextMsg).getVectorClock()) && pendingData.get(host).containsKey(((UpdateNotification) nextMsg).getvUp())){

                }
            }
            //TODO...
        }
        //TODO try to execute more operations (only update, never flush)
    }

    @Override
    void onDataMessage(DataMessage msg, Host host, short sourceProto, int channelId)
    {
        InetAddress source = host.getAddress();
        Queue<ProtoMessage> hostMetadata = pendingMetadata.get(source);
        Map<Integer, DataMessage> hostData = pendingData.get(source);

        ProtoMessage peek = hostMetadata.peek();
        if (peek == null) //Queue empty, no metadata
        {
            if (canExec(msg.getVectorClock()))
                startExec(msg, source);
            else
                hostData.put(msg.getvUp(), msg);
        }
        else if (peek instanceof UpdateNotification)
        {
            UpdateNotification uN = (UpdateNotification) hostMetadata.peek();
            if (uN.getvUp() == msg.getvUp())
                if (canExec(msg.getVectorClock()))
                {
                    hostMetadata.remove();
                    startExec(msg, source);
                }
                else
                    hostData.put(msg.getvUp(), msg);
            else if (uN.getvUp() < msg.getvUp())
                hostData.put(msg.getvUp(), msg);
            else
            {
                logger.error("Unexpected payload {} : {}", uN.getvUp(), msg.getvUp());
                throw new AssertionError();
            }
        }
        //If peek is metadataflush, do nothing. It means an operation is executing
    }

    @Override
    void onMetadataFlush(MetadataFlush msg, Host host, short sourceProto, int channelId)
    {
        //TODO for each entry
        //TODO If queue empty, increase clock immediately, else add to queue
        //TODO If executed, check queue to see if can execute more
    }

    @Override
    void onUpdateNotification(UpdateNotification msg, Host host, short sourceProto, int channelId)
    {
        onMetadataFlush(msg.getMf(), host, sourceProto, channelId);

        //TODO if queue empty, try to execute - if fail, add to queue. if not empty, add to queue
        //TODO if no data, cant execute.
        //TODO If executed (executing?), ignore....
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
        //mutation = Mutation.serializer.deserialize(
        // new DataInputBuffer(byteBuf.nioBuffer(), false), MessagingService.VERSION_40);
        //DataOutputBuffer buffer = new DataOutputBuffer();
        //Mutation.serializer.serialize(mutation, buffer, MessagingService.VERSION_40);

        try
        {
            String partition = mutation.getKeyspaceName();
            UpdateNotification not = new UpdateNotification(GenericProxy.myAddr,
                                                            vUp, partition, objectClock, null, null);
            sendMessage(not, null, clientChannel);
            DataMessage dataMessage = new DataMessage(mutation, objectClock, vUp);
            targets.get(partition).forEach(h -> sendMessage(dataMessage, h));
        }
        catch (Exception e)
        {
            logger.error("Exception in ship: " + e.getMessage());
            throw new AssertionError(e);
        }
    }
}
