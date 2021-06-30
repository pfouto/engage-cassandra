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

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.service.CassandraDaemon;
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
import pfouto.timers.LogTimer;
import pfouto.timers.ReconnectTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.simpleclientserver.SimpleClientChannel;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ServerDownEvent;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ServerFailedEvent;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ServerUpEvent;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

public abstract class GenericProxy extends GenericProtocol
{
    public static final String ENGAGE_SERVER_PORT = "1500";
    public static final String ENGAGE_PEER_PORT = "2600";

    public static final int RECONNECT_INTERVAL = 5000;

    public static final GenericProxy instance;
    public static final InetAddress myAddr;
    public static final Protocol protocol;
    private static final Config conf;

    private static final Logger logger = LoggerFactory.getLogger(GenericProxy.class);
    int clientChannel;
    int peerChannel;
    Map<String, List<Host>> targets;
    Set<Host> all;

    public GenericProxy(String name)
    {
        super(name, (short) 100);
        targets = new HashMap<>();
        all = new HashSet<>();
        Runtime.getRuntime().addShutdownHook(new Thread(this::storeClocks));
    }

    abstract void storeClocks();

    public abstract void blockUntil(ByteBuffer c);

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException
    {
        try
        {
            registerTimerHandler(ReconnectTimer.TIMER_ID, this::onReconnectTimer);
            registerTimerHandler(LogTimer.TIMER_ID, this::onLogTimer);
            {
                logger.info("GenericProxy connecting to metadata service");
                Properties clientProps = new Properties();
                clientProps.put(SimpleClientChannel.ADDRESS_KEY, "127.0.0.1");
                clientProps.put(SimpleClientChannel.PORT_KEY, ENGAGE_SERVER_PORT);
                clientProps.put(SimpleClientChannel.HEARTBEAT_INTERVAL_KEY, "3000");
                clientProps.put(SimpleClientChannel.HEARTBEAT_TOLERANCE_KEY, "10000");

                clientChannel = createChannel(SimpleClientChannel.NAME, clientProps);

                registerChannelEventHandler(clientChannel, ServerUpEvent.EVENT_ID, this::onServerUp);
                registerChannelEventHandler(clientChannel, ServerDownEvent.EVENT_ID, this::onServerDown);
                registerChannelEventHandler(clientChannel, ServerFailedEvent.EVENT_ID, this::onServerFailed);

                registerMessageSerializer(clientChannel, TargetsMessage.MSG_ID, TargetsMessage.serializer);
                registerMessageHandler(clientChannel, TargetsMessage.MSG_ID, this::uponTargetsMessage, this::onMessageFailed);
                registerMessageSerializer(clientChannel, MetadataFlush.MSG_ID, MetadataFlush.serializer);
                registerMessageHandler(clientChannel, MetadataFlush.MSG_ID, this::onMetadataFlush, this::onMessageFailed);
                registerMessageSerializer(clientChannel, UpdateNot.MSG_ID, UpdateNot.serializer);
                registerMessageHandler(clientChannel, UpdateNot.MSG_ID, this::onUpdateNotification, this::onMessageFailed);

                openConnection(null, clientChannel);
            }

            if (protocol != Protocol.edgegage)
            {
                logger.info("GenericProxy creating peer channel");
                InetAddress peerAddr = DatabaseDescriptor
                                       .getNetworkInterfaceAddress(conf.peer_interface,
                                                                   "peer_interface", false);
                Properties peerProps = new Properties();
                peerProps.put(TCPChannel.ADDRESS_KEY, peerAddr.getHostAddress());
                peerProps.put(TCPChannel.PORT_KEY, ENGAGE_PEER_PORT);
                peerProps.put(SimpleClientChannel.HEARTBEAT_INTERVAL_KEY, "3000");
                peerProps.put(SimpleClientChannel.HEARTBEAT_TOLERANCE_KEY, "10000");
                peerChannel = createChannel(TCPChannel.NAME, peerProps);

                registerChannelEventHandler(peerChannel, OutConnectionFailed.EVENT_ID, this::onOutConnectionFailed);
                registerChannelEventHandler(peerChannel, OutConnectionDown.EVENT_ID, this::onOutConnectionDown);
                registerChannelEventHandler(peerChannel, InConnectionDown.EVENT_ID, this::onInConnectionDown);
                registerChannelEventHandler(peerChannel, OutConnectionUp.EVENT_ID, this::onOutConnectionUp);
                registerChannelEventHandler(peerChannel, InConnectionUp.EVENT_ID, this::onInConnectionUp);

                registerMessageSerializer(peerChannel, DataMessage.MSG_ID, DataMessage.serializer);
                registerMessageSerializer(peerChannel, StabMessage.MSG_ID, StabMessage.serializer);

                registerMessageHandler(peerChannel, DataMessage.MSG_ID, this::onDataMessage, this::onMessageFailed);
                registerMessageHandler(peerChannel, StabMessage.MSG_ID, this::onStabMessage, this::onMessageFailed);
            }

            registerRequestHandler(MutationFinished.REQ_ID, this::onMutationFinished);

            setupPeriodicTimer(new LogTimer(), 10000, 15000);
            internalInit();
        }
        catch (Exception e)
        {
            logger.error("Exception in GenericProxy init: " + e.getMessage());
            CassandraDaemon.stop(null);
        }
    }

    abstract void internalInit() throws HandlerRegistrationException;

    abstract void onMutationFinished(MutationFinished request, short sourceProto);

    abstract void onDataMessage(DataMessage msg, Host host, short sourceProto, int channelId);

    abstract void onMetadataFlush(MetadataFlush msg, Host host, short sourceProto, int channelId);

    abstract void onUpdateNotification(UpdateNot msg, Host host, short sourceProto, int channelId);

    abstract void onStabMessage(StabMessage msg, Host host, short sourceProto, int channelId);

    private void onMessageFailed(ProtoMessage protoMessage, Host host, short destProto, Throwable reason, int channel)
    {
        logger.error("Message failed to " + host + ", " + protoMessage + ": " + reason.getMessage());
    }

    private void uponTargetsMessage(TargetsMessage msg, Host host, short i, int i1)
    {
        logger.info("TargetsMsg Received: " + msg);

        try
        {
            for (Map.Entry<String, List<String>> entry : msg.getMap().entrySet())
            {
                List<Host> partitionHosts = new LinkedList<>();
                for (String addr : entry.getValue())
                    partitionHosts.add(new Host(InetAddress.getByName(addr), Integer.parseInt(ENGAGE_PEER_PORT)));
                targets.put(entry.getKey(), partitionHosts);
            }
            for (String s : msg.getAll())
            {
                all.add(new Host(InetAddress.getByName(s), Integer.parseInt(ENGAGE_PEER_PORT)));
            }
        }
        catch (Exception e)
        {
            logger.error("Error parsing targets message: ", e);
            CassandraDaemon.stop(null);
        }

        createConnections(msg);
    }

    abstract void createConnections(TargetsMessage tm);

    private void onOutConnectionUp(OutConnectionUp event, int channelId)
    {
        logger.info("Connected out to " + event.getNode());
    }

    private void onOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId)
    {
        logger.warn("Failed connecting out to " + event.getNode() + " : " + event.getCause().getLocalizedMessage() +
                    ", retrying in " + RECONNECT_INTERVAL);
        setupTimer(new ReconnectTimer(event.getNode()), RECONNECT_INTERVAL);
    }

    private void onReconnectTimer(ReconnectTimer timer, long uId)
    {
        logger.info("Reconnecting out to " + timer.getNode());
        openConnection(timer.getNode(), peerChannel);
    }

    private void onLogTimer(LogTimer timer, long uId)
    {
        internalOnLogTimer();
    }

    abstract void internalOnLogTimer();

    private void onOutConnectionDown(OutConnectionDown ev, int channelId)
    {
        logger.warn("Lost connection out to {} ({}), reconnecting in {}", ev.getNode(), ev.getCause(), RECONNECT_INTERVAL);
        setupTimer(new ReconnectTimer(ev.getNode()), RECONNECT_INTERVAL);
    }

    private void onInConnectionUp(InConnectionUp event, int channelId)
    {
        logger.info("Connection in up from " + event.getNode());
    }

    private void onInConnectionDown(InConnectionDown event, int channelId)
    {
        logger.warn("Connection in down from {} ( {} )", event.getNode(), event.getCause());
    }

    private void onServerFailed(ServerFailedEvent event, int i)
    {
        logger.error("Server connection failed, stopping");
        CassandraDaemon.stop(null);
    }

    private void onServerDown(ServerDownEvent event, int i)
    {
        logger.error("Server connection lost: " + event.getCause());
        CassandraDaemon.stop(null);
    }

    private void onServerUp(ServerUpEvent event, int i)
    {
        logger.warn("Connected to server");
    }

    public abstract int parseAndShip(Mutation mutation, byte[] currentClockData,
                                     byte[] clientClockData, BufferCell clockCell);

    public enum Protocol
    {bayou, saturn, edgegage, engage}

    static
    {
        conf = DatabaseDescriptor.getRawConfig();
        protocol = Protocol.valueOf(conf.protocol);
        myAddr = FBUtilities.getJustBroadcastNativeAddress();
        switch (protocol)
        {
            case bayou:
                instance = new BayouProxy();
                break;
            case engage:
                instance = new EngageProxy();
                break;
            case saturn:
                instance = new SaturnProxy();
                break;
            case edgegage:
                instance = new EdgegageProxy();
                break;
            default:
                instance = null;
        }
    }
}
