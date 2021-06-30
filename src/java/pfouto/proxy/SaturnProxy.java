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
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class SaturnProxy extends GenericProxy
{
    private static final Logger logger = LoggerFactory.getLogger(SaturnProxy.class);
    final Object counterLock = new Object();
    ConcurrentMap<InetAddress, MutableInteger> remoteTimestamps = new ConcurrentHashMap<>();
    int localCounter = 0;
    int executing = 0;

    Queue<UpdateNot> pendingMetadata;
    Map<InetAddress, Map<Integer, DataMessage>> pendingData;

    Map.Entry<InetAddress, Integer> lastExec;

    Set<Host> peers = new HashSet<>();

    public SaturnProxy()
    {
        super("SaturnProxy");
        pendingMetadata = new LinkedList<>();
        pendingData = new HashMap<>();
        lastExec = new AbstractMap.SimpleEntry<>(myAddr, -1);
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
                for(int i = 0;i<mapSize;i++)
                {
                    InetAddress addr = Inet4Address.getByAddress(dis.readNBytes(4));
                    int val = dis.readInt();
                    remoteTimestamps.put(addr, new MutableInteger(val));
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
        if (!pendingMetadata.isEmpty())
            logger.error("Shutting down with pending metadata!");
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
                dos.writeInt(remoteTimestamps.size());
                for (Map.Entry<InetAddress, MutableInteger> entry : remoteTimestamps.entrySet())
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


    private boolean smaller(Inet4Address adr1, Inet4Address adr2)
    {
        byte[] ba1 = adr1.getAddress();
        byte[] ba2 = adr2.getAddress();

        // we have 2 ips of the same type, so we have to compare each byte
        for (int i = 0; i < ba1.length; i++)
        {
            int b1 = unsignedByteToInt(ba1[i]);
            int b2 = unsignedByteToInt(ba2[i]);
            if (b1 != b2) return b1 < b2;
        }
        return false;
    }

    private int unsignedByteToInt(byte b)
    {
        return (int) b & 0xFF;
    }

    private boolean smallerLabel(InetAddress addr, int vUp)
    {
        return vUp < lastExec.getValue() ||
               (vUp == lastExec.getValue() && smaller((Inet4Address) addr, (Inet4Address) lastExec.getKey()));
    }

    @Override
    void onMutationFinished(MutationFinished request, short sourceProto)
    {
        executing--;
        MutableInteger value = remoteTimestamps.computeIfAbsent(request.getSource(), k -> new MutableInteger());
        synchronized (value)
        {
            value.setValue(Math.max(value.getValue(), request.getvUp()));
            value.notifyAll();
        }
        tryExecNext();
    }

    private void tryExecNext()
    {
        boolean executed = true;
        while (executed)
        {
            executed = false;
            UpdateNot not = pendingMetadata.peek();
            if (not == null) break;
            Map<Integer, DataMessage> hostData = pendingData.computeIfAbsent(not.getSource(), k -> new HashMap<>());
            if (hostData.containsKey(not.getvUp()) && (executing == 0 || smallerLabel(not.getSource(), not.getvUp())))
            {
                executing++;
                synchronized (counterLock)
                {
                    localCounter = Math.max(localCounter, not.getvUp());
                }
                lastExec = new AbstractMap.SimpleEntry<>(not.getSource(), not.getvUp());

                pendingMetadata.remove();
                DataMessage remove = hostData.remove(not.getvUp());

                Stage.MUTATION.maybeExecuteImmediately(() -> { //This block will run on mutation threadpool
                    try
                    {
                        remove.getMutation().apply();
                        //Once finished, "onMutationFinished" is called
                        sendRequest(new MutationFinished(not.getvUp(), not.getSource()), this.getProtoId());
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

    @Override
    void onUpdateNotification(UpdateNot msg, Host host, short sourceProto, int channelId)
    {
        logger.debug("{} received from {}", msg, host);
        pendingMetadata.add(msg);
        if (pendingMetadata.size() == 1) //If previous metadata in queue, then no point trying anything
            tryExecNext();
    }

    @Override
    void onDataMessage(DataMessage msg, Host host, short sourceProto, int channelId)
    {
        logger.debug("{} received from {}", msg, host);
        InetAddress source = host.getAddress();
        Map<Integer, DataMessage> hostData = pendingData.computeIfAbsent(source, k -> new HashMap<>());

        hostData.put(msg.getvUp(), msg);
        tryExecNext();
    }


    @Override
    void createConnections(TargetsMessage tm)
    {
        for (Host h : all)
        {
            if (peers.add(h))
                setupTimer(new ReconnectTimer(h), 2000);
        }
    }

    @Override
    void internalOnLogTimer()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Clk ");
        sb.append(localCounter);
        sb.append(" {");
        remoteTimestamps.forEach((k, v) -> {
            String last = k.getHostAddress().substring(10);
            sb.append(last).append('=').append(v.getValue()).append(' ');
        });
        sb.append('}');

        int pendingDataTotal = 0;
        for (Map.Entry<InetAddress, Map<Integer, DataMessage>> entry : pendingData.entrySet())
        {
            pendingDataTotal += entry.getValue().size();
        }
        int pendingMetadataTotal = pendingMetadata.size();
        if (pendingMetadataTotal > 0 || pendingDataTotal > 0)
            sb.append("Pending: m-").append(pendingMetadataTotal).append(" d-").append(pendingDataTotal);
        logger.warn(sb.toString());

        pendingData.forEach((k, v) -> {
            if (!v.isEmpty())
                if (logger.isDebugEnabled())
                    logger.debug("PendingData {}: {}", k, v.size());
        });
    }

    @Override
    public int parseAndShip(Mutation mutation, byte[] currentClockData, byte[] clientClockData, BufferCell clockCell)
    {
        try
        {
            //Parse clocks
            ByteArrayInputStream cbais = new ByteArrayInputStream(clientClockData);
            Map.Entry<Inet4Address, Integer> clientClock = entryFromInputStream(cbais);
            cbais.close();

            //Alter mutation with new clock
            clockCell.setValue(entryToBuffer(clientClock).flip());
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
                UpdateNot not = new UpdateNot(myAddr, vUp, partition, new Clock(), null, null);
                sendMessage(clientChannel, not, null);
                DataMessage dataMessage = new DataMessage(mutation, new Clock(), vUp);
                if (partition.equals("migration"))
                    peers.forEach(h -> sendMessage(peerChannel, dataMessage, h));
                else
                    targets.get(partition).forEach(h -> sendMessage(peerChannel, dataMessage, h));
            }
            return vUp;
        }
        catch (Exception e)
        {
            logger.error("Exception in ship: " + e.getMessage());
            e.printStackTrace();
            throw new AssertionError(e);
        }
    }

    @Override
    public void blockUntil(ByteBuffer c)
    {
        Map.Entry<Inet4Address, Integer> clientEntry;
        try
        {
            clientEntry = entryFromInputStream(new ByteArrayInputStream(c.array()));
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new RuntimeException("Could not read client clock");
        }

        if (clientEntry.getKey().equals(myAddr)) return;
        ImmutableInteger localValue = getRemoteTimestamp(clientEntry.getKey());
        if (localValue.getValue() >= clientEntry.getValue()) return;
        synchronized (localValue)
        {
            while (localValue.getValue() < clientEntry.getValue())
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

    private ImmutableInteger getRemoteTimestamp(InetAddress pos)
    {
        return remoteTimestamps.computeIfAbsent(pos, k -> new MutableInteger());
    }

    private Map.Entry<Inet4Address, Integer> entryFromInputStream(InputStream data) throws IOException
    {
        try (DataInputStream dis = new DataInputStream(data))
        {
            Inet4Address addr = (Inet4Address) Inet4Address.getByAddress(dis.readNBytes(4));
            int val = dis.readInt();
            return new AbstractMap.SimpleEntry<>(addr, val);
        }
    }

    private ByteBuffer entryToBuffer(Map.Entry<Inet4Address, Integer> entry)
    {
        ByteBuffer allocate = ByteBuffer.allocate(8);
        allocate.put(entry.getKey().getAddress());
        allocate.putInt(entry.getValue());
        return allocate;
    }

    @Override
    void onStabMessage(StabMessage msg, Host host, short sourceProto, int channelId)
    {
        throw new AssertionError("Unexpected message " + msg + " from " + host);
    }

    @Override
    void onMetadataFlush(MetadataFlush msg, Host host, short sourceProto, int channelId)
    {
        throw new AssertionError("Unexpected message " + msg + " from " + host);
    }

    @Override
    void internalInit()
    {
    }
}
