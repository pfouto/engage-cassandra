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
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
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
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
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
import pfouto.timers.StabTimer;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class BayouProxy extends GenericProxy
{
    private static final Logger logger = LoggerFactory.getLogger(BayouProxy.class);
    final Object counterLock = new Object();
    Map<InetAddress, Queue<ProtoMessage>> pendingData;
    Map<InetAddress, MutableInteger> executing;
    Map<InetAddress, PriorityQueue<Integer>> outOfOrderExecuted;
    Map<Host, MutableInteger> peersMissing;
    ConcurrentMap<InetAddress, MutableInteger> globalClock = new ConcurrentHashMap<>();
    int bayouStabMs;
    int localCounter = 0;

    public BayouProxy()
    {
        super("BayouProxy");
        pendingData = new HashMap<>();
        executing = new HashMap<>();
        outOfOrderExecuted = new HashMap<>();
        peersMissing = new HashMap<>();
    }

    @Override
    void internalInit() throws HandlerRegistrationException
    {
        registerTimerHandler(StabTimer.TIMER_ID, this::uponStabTimer);
    }

    private void tryExecQueue(InetAddress source)
    {
        Queue<ProtoMessage> hostData = pendingData.computeIfAbsent(source, k -> new LinkedList<>());
        boolean executed = true;
        boolean tryAll = false;
        ProtoMessage peek;
        while (executed && ((peek = hostData.peek()) != null))
        {
            executed = false;
            if (peek instanceof StabMessage) //MF message
            {
                StabMessage sm = (StabMessage) peek;
                MutableInteger globalClockPos = globalClock.computeIfAbsent(source, k -> new MutableInteger());
                MutableInteger executingClockPos = executing.computeIfAbsent(source, k -> new MutableInteger());
                if (sm.getvUp() > executingClockPos.getValue() &&
                    executingClockPos.getValue() == globalClockPos.getValue()) //Means every update has already finished
                {
                    hostData.remove();
                    //Can just update everything, nothing is pending
                    executingClockPos.setValue(sm.getvUp());
                    synchronized (globalClockPos)
                    {
                        globalClockPos.setValue(sm.getvUp());
                        globalClockPos.notifyAll();
                    }
                    logger.debug("Executed Stab {} {}", source, sm.getvUp());
                    executed = true;
                    tryAll = true;
                } //Else, we wait for the update to finish, and then apply the MF
            }
            else //Update message
            {
                DataMessage dm = (DataMessage) peek;
                if (canExec(dm.getVectorClock())) //Can exec mutation
                {
                    hostData.remove();
                    MutableInteger executingClockPos = executing.computeIfAbsent(source, k -> new MutableInteger());
                    if (dm.getvUp() != executingClockPos.getValue() + 1)
                    {
                        logger.error("Executing unexpected op {} {}", dm.getvUp(), executing.get(source));
                        throw new AssertionError();
                    }
                    executingClockPos.setValue(dm.getvUp());
                    Stage.MUTATION.maybeExecuteImmediately(() -> { //This block will run on mutation threadpool
                        try
                        {
                            dm.getMutation().apply();
                            //Once finished, "onMutationFinished" is called
                            sendRequest(new MutationFinished(dm.getvUp(), source), this.getProtoId());
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
            pendingData.keySet().forEach(this::tryExecQueue);
        }
        else //Else, just add to the outOfOrder struct
        {
            ooo.add(vUp);
        }
    }

    @Override
    void onDataMessage(DataMessage msg, Host host, short sourceProto, int channelId)
    {
        addToQueueAndTryExec(msg, host);
    }

    @Override
    void onStabMessage(StabMessage msg, Host host, short sourceProto, int channelId)
    {
        MutableInteger executingClockPos = executing.computeIfAbsent(host.getAddress(), k -> new MutableInteger());
        if (executingClockPos.getValue() < msg.getvUp())
            addToQueueAndTryExec(msg, host);
    }

    private void addToQueueAndTryExec(ProtoMessage msg, Host from)
    {
        logger.debug("{} received from {}", msg, from);
        InetAddress source = from.getAddress();
        Queue<ProtoMessage> queue = pendingData.computeIfAbsent(source, k -> new LinkedList<>());
        queue.add(msg);
        if (queue.size() == 1)
        {
            tryExecQueue(source);
        }
    }

    @Override
    void createConnections(TargetsMessage tm)
    {
        for (Map.Entry<String, List<Host>> entry : targets.entrySet())
            for (Host h : entry.getValue())
                if (peersMissing.putIfAbsent(h, new MutableInteger(0)) == null)
                    openConnection(h, peerChannel);

        bayouStabMs = tm.getBayouStabMs();
        setupPeriodicTimer(new StabTimer(), bayouStabMs, bayouStabMs);
    }

    @Override
    void internalOnLogTimer()
    {
        logger.info("Clock {} {}", localCounter, globalClock);
        pendingData.forEach((k, v) -> {
            if (!v.isEmpty())
            {
                logger.info("Pending {}: {}", k, v.size());
                logger.debug("First: " + v.peek());
            }
        });
    }

    private void uponStabTimer(StabTimer timer, long tId)
    {
        peersMissing.forEach((k, v) -> {
            if (v.getValue() != 0)
            {
                sendMessage(peerChannel, new StabMessage(v.getValue()), k);
                v.setValue(0);
            }
        });
    }

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
                DataMessage dataMessage = new DataMessage(mutation, objectClock, vUp);
                List<Host> hosts = targets.get(partition);

                boolean migration = partition.equals("migration");
                peersMissing.forEach((k, v) -> {
                    if (migration || hosts.contains(k))
                    {
                        if (v.getValue() != 0)
                        {
                            sendMessage(peerChannel, new StabMessage(v.getValue()), k);
                            v.setValue(0);
                        }
                        sendMessage(peerChannel, dataMessage, k);
                    }
                    else
                        v.setValue(vUp);
                });
            }
            return vUp;
        }
        catch (Exception e)
        {
            logger.error("Exception in ship: " + e.getMessage());
            throw new AssertionError(e);
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

    @Override
    void onMetadataFlush(MetadataFlush msg, Host from, short sourceProto, int channelId)
    {
        throw new AssertionError("Unexpected message " + msg + " from " + from);
    }

    @Override
    void onUpdateNotification(UpdateNot msg, Host from, short sourceProto, int channelId)
    {
        throw new AssertionError("Unexpected message " + msg + " from " + from);
    }
}
