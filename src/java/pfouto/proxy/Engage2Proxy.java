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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.utils.FBUtilities;
import pfouto.Clock;
import pfouto.ImmutableInteger;
import pfouto.MutableInteger;
import pfouto.ipc.MutationFinished;
import pfouto.messages.side.DataMessage;
import pfouto.messages.side.StabMessage;
import pfouto.messages.up.MetadataFlush;
import pfouto.messages.up.TargetsMessage;
import pfouto.messages.up.UpdateNot;
import pfouto.timers.ReconnectTimer;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class Engage2Proxy extends GenericProxy
{
    private static final Logger logger = LoggerFactory.getLogger(Engage2Proxy.class);
    final Object counterLock = new Object();
    Map<InetAddress, Queue<ProtoMessage>> pendingMetadata;
    Map<InetAddress, Map<Integer, DataMessage>> pendingData;
    Map<InetAddress, MutableInteger> executing;
    Map<InetAddress, PriorityQueue<Integer>> outOfOrderExecuted;
    ConcurrentMap<InetAddress, MutableInteger> globalClock = new ConcurrentHashMap<>();
    int localCounter = 0;
    Set<Host> peers = new HashSet<>();

    public Engage2Proxy()
    {
        super("Engage2Proxy");
        pendingMetadata = new HashMap<>();
        pendingData = new HashMap<>();
        executing = new HashMap<>();
        outOfOrderExecuted = new HashMap<>();
        readClocks();
    }

    private void readClocks()
    {
        File file = new File("data/clock");
        if (!file.exists()) return;
        try (FileInputStream fis = new FileInputStream(file))
        {
            try (DataInputStream dis = new DataInputStream(fis))
            {
                localCounter = dis.readInt();
                int mapSize = dis.readInt();
                for (int i = 0; i < mapSize; i++)
                {
                    InetAddress addr = Inet4Address.getByAddress(dis.readNBytes(4));
                    int val = dis.readInt();
                    globalClock.put(addr, new MutableInteger(val));
                    executing.put(addr, new MutableInteger(val));
                }
            }
        }
        catch (IOException ioe)
        {
            logger.error("Error reading clocks", ioe);
            ioe.printStackTrace();
        }
        file.delete();
        logger.warn("Values read");
    }


    @Override
    void storeClocks()
    {
        for (Map.Entry<InetAddress, Queue<ProtoMessage>> entry : pendingMetadata.entrySet())
        {
            if (!entry.getValue().isEmpty())
                logger.error("Shutting down with pending metadata!");
        }
        for (Map.Entry<InetAddress, Map<Integer, DataMessage>> entry : pendingData.entrySet())
        {
            if (!entry.getValue().isEmpty())
                logger.error("Shutting down with pending data!");
        }

        File file = new File("data/clock");
        try (FileOutputStream fos = new FileOutputStream(file))
        {
            if (!file.exists())
            {
                file.createNewFile();
            }
            try (DataOutputStream dos = new DataOutputStream(fos))
            {
                dos.writeInt(localCounter);
                dos.writeInt(globalClock.size());
                for (Map.Entry<InetAddress, MutableInteger> entry : globalClock.entrySet())
                {
                    InetAddress k = entry.getKey();
                    MutableInteger v = entry.getValue();
                    dos.write(k.getAddress());
                    dos.writeInt(v.getValue());
                }
            }
        }
        catch (IOException ioe)
        {
            logger.error("Error writing clocks", ioe);
            ioe.printStackTrace();
        }
        logger.warn("Values written");
    }

    private void tryExecQueue(InetAddress source)
    {
        Queue<ProtoMessage> hostMetadata = pendingMetadata.computeIfAbsent(source, k -> new LinkedList<>());
        boolean executed = true;
        boolean tryAll = false;
        ProtoMessage peek;
        while (executed && ((peek = hostMetadata.peek()) != null))
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
                    logger.debug("Executed metadata {} {}", source, newClock);
                    executed = true;
                    tryAll = true;
                } //Else, we wait for the update to finish, and then apply the MF
            }
            else //Update message
            {
                UpdateNot un = (UpdateNot) peek;
                Map<Integer, DataMessage> hostData = pendingData.computeIfAbsent(source, k -> new HashMap<>());
                if (hostData.containsKey(un.getvUp()) && canExec(un.getVectorClock())) //Can exec mutation
                {
                    hostMetadata.remove();
                    DataMessage data = hostData.remove(un.getvUp());
                    MutableInteger executingClockPos = executing.computeIfAbsent(source, k -> new MutableInteger());
                    if (data.getvUp() != executingClockPos.getValue() + 1)
                    {
                        logger.error("Executing unexpected op {} {} {}", source, data.getvUp(), executing.get(source));
                        throw new AssertionError();
                    }
                    executingClockPos.setValue(data.getvUp());
                    Stage.MUTATION.maybeExecuteImmediately(() -> { //This block will run on mutation threadpool
                        try
                        {
                            data.getMutation().apply();
                            //Once finished, "onMutationFinished" is called
                            sendRequest(new MutationFinished(data.getvUp(), source), this.getProtoId());
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
        if (tryAll)
            pendingData.keySet().forEach(this::tryExecQueue);
    }

    @Override
    void onMutationFinished(MutationFinished request, short sourceProto)
    {
        InetAddress source = request.getSource();
        int vUp = request.getvUp();
        if(logVisibility)
            logger.info("OP_EXEC " + source.getHostAddress() + ' ' + vUp);
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
            logger.debug("Executed data {} {}", source, highestVUp);
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
        logger.debug("{} received from {}", msg, host);
        InetAddress source = host.getAddress();
        Map<Integer, DataMessage> hostData = pendingData.computeIfAbsent(source, k -> new HashMap<>());

        hostData.put(msg.getvUp(), msg);
        if (hostData.size() == 1) //If previous data is stil in map, means this is not the next update
            tryExecQueue(source);
    }

    @Override
    void onMetadataFlush(MetadataFlush msg, Host host, short sourceProto, int channelId)
    {
        logger.debug("{} received from {}", msg, host);
        for (InetAddress k : msg.getUpdates().keySet())
        {
            Queue<ProtoMessage> hostMetadata = pendingMetadata.computeIfAbsent(k, key -> new LinkedList<>());
            hostMetadata.add(msg);
            if (hostMetadata.size() == 1)
                tryExecQueue(k); //Will always exec
        }
    }

    @Override
    void onUpdateNotification(UpdateNot msg, Host host, short sourceProto, int channelId)
    {
        logger.debug("{} received from {}", msg, host);
        if (msg.getMf() != null)
            onMetadataFlush(msg.getMf(), host, sourceProto, channelId);

        InetAddress source = msg.getSource();
        Queue<ProtoMessage> hostMetadata = pendingMetadata.computeIfAbsent(source, k -> new LinkedList<>());
        if (executing.computeIfAbsent(source, k -> new MutableInteger()).getValue() < msg.getvUp())
        {
            hostMetadata.add(msg);
            if (hostMetadata.size() == 1) //If previous metadata in queue, then no point trying anything
                tryExecQueue(source);
        } //If already executing, ignore message
    }


    @Override
    void createConnections(TargetsMessage tm)
    {
        for (Host h : all)
            if (peers.add(h))
                setupTimer(new ReconnectTimer(h), 2000);
    }

    @Override
    void internalOnLogTimer()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Clk ");
        sb.append(localCounter);
        sb.append(" {");
        globalClock.forEach((k, v) -> {
            String last = k.getHostAddress().substring(10);
            sb.append(last).append('=').append(v.getValue()).append(' ');
        });
        sb.append('}');

        int pendingDataTotal = 0;
        for (Map.Entry<InetAddress, Map<Integer, DataMessage>> entry : pendingData.entrySet())
        {
            pendingDataTotal += entry.getValue().size();
        }
        int pendingMetadataTotal = 0;
        for (Map.Entry<InetAddress, Queue<ProtoMessage>> entry : pendingMetadata.entrySet())
        {
            pendingMetadataTotal += entry.getValue().size();
        }
        if (pendingMetadataTotal > 0 || pendingDataTotal > 0)
            sb.append("Pending: m-").append(pendingMetadataTotal).append(" d-").append(pendingDataTotal);
        logger.warn(sb.toString());


        pendingData.forEach((k, v) -> {
            if (!v.isEmpty() || !pendingMetadata.get(k).isEmpty())
            {
                logger.debug("Pending {}: {}/{}", k, v.size(), pendingMetadata.get(k).size());
                logger.debug("First: " + pendingMetadata.get(k).peek());
            }
        });
    }

    //Called by Cassandra query executor thread pool. Not by babel threads!
    @Override
    public int parseAndShip(Mutation mutation, byte[] currentClockData, byte[] clientClockData, BufferCell clockCell)
    {
        try
        {
            //Parse clocks
            Clock clientClock, objectClock;
            if (currentClockData != null)
            {
                ByteArrayInputStream obais = new ByteArrayInputStream(currentClockData);
                objectClock = Clock.fromInputStream(obais);
                obais.close();
            }
            else
                objectClock = new Clock();

            ByteArrayInputStream cbais = new ByteArrayInputStream(clientClockData);
            clientClock = Clock.fromInputStream(cbais);
            cbais.close();

            //Update object clock
            objectClock.merge(clientClock);
            //Alter mutation with new clock
            clockCell.setValue(objectClock.toBuffer().flip());

            //Need to synchronized from counter++ until ship, to make sure ops are shipped in the correct order...
            long timestamp;
            int vUp;
            synchronized (counterLock)
            {
                vUp = ++localCounter;
                timestamp = FBUtilities.timestampMicros();
                clockCell.setTimestamp(timestamp);

                logger.debug("Shipping and executing local {}", vUp);
                String partition = mutation.getKeyspaceName();
                UpdateNot not = new UpdateNot(myAddr, vUp, partition, objectClock, null, null);
                sendMessage(clientChannel, not, null);
                DataMessage dataMessage = new DataMessage(mutation, new Clock(), vUp);
                if (partition.equals("migration"))
                    peers.forEach(h -> sendMessage(peerChannel, dataMessage, h));
                else
                    targets.get(partition).forEach(h -> sendMessage(peerChannel, dataMessage, h));
            }
            if(logVisibility)
                logger.info("OP_GEN " + myAddr.getHostAddress() + ' ' + vUp);
            return vUp;
        }
        catch (Exception e)
        {
            logger.error("Exception in ship: " + e.getMessage());
            throw new AssertionError(e);
        }
    }

    @Override
    public void blockUntil(ByteBuffer c)
    {
        Clock clientClock;
        try
        {
            clientClock = Clock.fromInputStream(new ByteArrayInputStream(c.array()));
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new RuntimeException("Could not read client clock");
        }

        for (Map.Entry<Inet4Address, Integer> entry : clientClock.getValue().entrySet())
        {
            //ignore my own entry (will always be up-to-date)
            Inet4Address key = entry.getKey();
            if (key.equals(myAddr)) continue; //Ignore my own entry
            Integer clientValue = entry.getValue();
            ImmutableInteger localValue = getClockValue(key);
            if (localValue.getValue() >= clientValue) continue; //If already satisfied, no need for locks
            synchronized (localValue)
            {
                while (localValue.getValue() < clientValue)
                {
                    try
                    {
                        localValue.wait();
                    }
                    catch (InterruptedException ignored)
                    {
                    }
                }
            }
        }
    }

    public ImmutableInteger getClockValue(InetAddress pos)
    {
        return globalClock.computeIfAbsent(pos, k -> new MutableInteger());
    }

    boolean canExec(Clock opClock)
    {
        for (Map.Entry<Inet4Address, Integer> opClockPos : opClock.getValue().entrySet())
        {
            if (!opClockPos.getKey().equals(myAddr) &&
                getClockValue(opClockPos.getKey()).getValue() < opClockPos.getValue())
                return false;
        }
        return true;
    }

    @Override
    void internalInit()
    {

    }

    @Override
    void onStabMessage(StabMessage msg, Host from, short sourceProto, int channelId)
    {
        throw new AssertionError("Unexpected message " + msg + " from " + from);
    }
}
