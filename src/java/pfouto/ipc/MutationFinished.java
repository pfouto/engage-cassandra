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

package pfouto.ipc;

import java.net.InetAddress;

import pfouto.messages.side.DataMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class MutationFinished extends ProtoRequest
{
    public static final short REQ_ID = 101;

    private final int vUp;
    private final String partition;
    private final InetAddress source;

    public MutationFinished(int vUp, String partition, InetAddress source)
    {
        super(REQ_ID);
        this.vUp = vUp;
        this.partition = partition;
        this.source = source;
    }

    public int getvUp()
    {
        return vUp;
    }

    public InetAddress getSource()
    {
        return source;
    }

    public String getPartition()
    {
        return partition;
    }
}
