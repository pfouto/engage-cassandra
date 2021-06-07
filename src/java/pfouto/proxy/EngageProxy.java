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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
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

    Map<InetAddress, MutableInteger> executing;
    Map<InetAddress, PriorityQueue<Integer>> outOfOrderExecuted;

    public EngageProxy()
    {
        super("EngageProxy");
    }

    private boolean canExec(Clock opClock)
    {
        for (Map.Entry<Inet4Address, Integer> opClockPos : opClock.getValue().entrySet())
            if (getClockValue(opClockPos.getKey()).getValue() < opClockPos.getValue())
                return false;
        return true;
    }

    private void tryExecQueue(InetAddress source)
    {
        Queue<ProtoMessage> hostMetadata = pendingMetadata.get(source);
        ProtoMessage peek = hostMetadata.peek();
        boolean executed = true;
        while (executed)
        {
            executed = false;
            if (peek instanceof MetadataFlush) //MF message
            {
                MetadataFlush mf = (MetadataFlush) peek;
                MutableInteger globalClockPos = globalClock.computeIfAbsent(source, k -> new MutableInteger());
                MutableInteger executingClockPos = executing.computeIfAbsent(source, k -> new MutableInteger());
                if (executingClockPos.getValue() == globalClockPos.getValue()) //Means every update has already finished
                {
                    hostMetadata.remove();
                    //Can just update everything, nothing is pending
                    Integer newClock = mf.getUpdates().get(source);
                    executingClockPos.setValue(newClock);
                    synchronized (globalClockPos)
                    {
                        globalClockPos.setValue(newClock);
                        globalClockPos.notifyAll();
                    }
                    executed = true;
                    pendingMetadata.keySet().forEach(this::tryExecQueue);
                } //Else, we wait for the update to finish, and then apply the MF
            }
            else //Update message
            {
                UpdateNotification un = (UpdateNotification) peek;
                Map<Integer, DataMessage> hostData = pendingData.get(source);
                if (hostData.containsKey(un.getvUp()) && canExec(un.getVectorClock())) //Can exec mutation
                {
                    hostMetadata.remove();
                    DataMessage data = hostData.remove(un.getvUp());
                    MutableInteger executingClockPos = executing.computeIfAbsent(source, k -> new MutableInteger());
                    if (data.getvUp() != executingClockPos.getValue() + 1)
                    {
                        logger.error("Executing unexpected op {} {}", data.getvUp(), executing.get(source));
                        throw new AssertionError();
                    }
                    executingClockPos.setValue(data.getvUp());
                    Stage.MUTATION.maybeExecuteImmediately(() -> { //This block will run on mutation threadpool
                        try
                        {
                            data.getMutation().apply();
                            //Once finished, "onMutationFinished" is called
                            sendRequest(new MutationFinished(data, source), this.getProtoId());
                        }
                        catch (Exception e)
                        {
                            logger.error("Failed to apply remote mutation locally : ", e);
                            throw new AssertionError(e);
                        }
                    });
                    executed = true;
                }
            }
        }
    }

    @Override
    void onMutationFinished(MutationFinished request, short sourceProto)
    {
        InetAddress source = request.getSource();
        int vUp = request.getMutationMessage().getvUp();
        MutableInteger cPos = globalClock.computeIfAbsent(source, k -> new MutableInteger());
        PriorityQueue<Integer> ooo = outOfOrderExecuted.computeIfAbsent(source, k -> new PriorityQueue<>());
        //If is next "executed" op, check for following finished ops and update clock
        if (vUp == (cPos.getValue() + 1))
        {
            int highestVUp = vUp;
            while (!ooo.isEmpty() && (ooo.peek() == (highestVUp + 1)))
            {
                highestVUp = ooo.remove();
            }
            //Update clock, notifying waiting ops
            synchronized (cPos)
            {
                cPos.setValue(highestVUp);
                cPos.notifyAll();
            }
            pendingMetadata.keySet().forEach(this::tryExecQueue);
        }
        else //Else, just add to the outOfOrder struct
        {
            ooo.add(vUp);
        }
    }

    @Override
    void onDataMessage(DataMessage msg, Host host, short sourceProto, int channelId)
    {
        InetAddress source = host.getAddress();
        Map<Integer, DataMessage> hostData = pendingData.get(source);

        hostData.put(msg.getvUp(), msg);
        if (hostData.size() == 1) //If previous data is stil in map, means this is not the next update
            tryExecQueue(source);
    }

    @Override
    void onMetadataFlush(MetadataFlush msg, Host host, short sourceProto, int channelId)
    {
        for (InetAddress k : msg.getUpdates().keySet())
        {
            Queue<ProtoMessage> hostMetadata = pendingMetadata.get(k);
            hostMetadata.add(msg);
            if (hostMetadata.size() == 1)
                tryExecQueue(k); //Will always exec
        }
    }

    @Override
    void onUpdateNotification(UpdateNotification msg, Host host, short sourceProto, int channelId)
    {
        onMetadataFlush(msg.getMf(), host, sourceProto, channelId);

        InetAddress source = host.getAddress();
        Queue<ProtoMessage> hostMetadata = pendingMetadata.get(source);
        if (executing.computeIfAbsent(source, k -> new MutableInteger()).getValue() < msg.getvUp())
        {
            hostMetadata.add(msg);
            if (hostMetadata.size() == 1) //If previous metadata in queue, then no point trying anything
                tryExecQueue(source);
        } //If already executing, ignore message
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
            UpdateNotification not = new UpdateNotification(myAddr, vUp, partition, objectClock, null, null);
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
