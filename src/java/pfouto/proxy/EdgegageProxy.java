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

import org.apache.cassandra.db.Mutation;
import pfouto.Clock;
import pfouto.ipc.MutationFinished;
import pfouto.messages.side.DataMessage;
import pfouto.messages.side.StabMessage;
import pfouto.messages.up.MetadataFlush;
import pfouto.messages.up.UpdateNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class EdgegageProxy extends GenericProxy
{

    public EdgegageProxy()
    {
        super("EdgegageProxy");
    }

    @Override
    void onMutationFinished(MutationFinished request, short sourceProto)
    {

    }

    @Override
    void onDataMessage(DataMessage msg, Host host, short sourceProto, int channelId)
    {

    }

    @Override
    void onMetadataFlush(MetadataFlush msg, Host host, short sourceProto, int channelId)
    {

    }

    @Override
    void onUpdateNotification(UpdateNotification msg, Host host, short sourceProto, int channelId)
    {

    }

    @Override
    void onStabMessage(StabMessage msg, Host host, short sourceProto, int channelId)
    {

    }

    @Override
    void createConnections()
    {

    }

    @Override
    public void ship(Mutation mutation, Clock objectClock, int vUp)
    {

    }
}
