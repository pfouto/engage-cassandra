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

package pfouto.messages.up;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import pfouto.Utils;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class TargetsMessage extends ProtoMessage
{

    public static final short MSG_ID = 201;
    public static final ISerializer<TargetsMessage> serializer = new ISerializer<>()
    {
        @Override
        public void serialize(TargetsMessage msg, ByteBuf out) throws IOException
        {
            Utils.serializeString(msg.myName, out);
            out.writeInt(msg.getMap().size());
            msg.getMap().forEach((k, v) -> {
                Utils.serializeString(k, out);
                out.writeInt(v.size());
                v.forEach(h -> Utils.serializeString(h, out));
            });
            out.writeInt(msg.getAll().size());
            msg.getAll().forEach((k,v) -> {
                Utils.serializeString(k, out);
                Utils.serializeString(v, out);
            });
            out.writeInt(msg.bayouStabMs);
        }

        @Override
        public TargetsMessage deserialize(ByteBuf in) throws IOException
        {
            String myName = Utils.deserializeString(in);
            int nElems = in.readInt();
            HashMap<String, List<String>> map = new HashMap<>();
            for (int i = 0; i < nElems; i++)
            {
                String key = Utils.deserializeString(in);
                int size = in.readInt();
                LinkedList<String> objects = new LinkedList<>();
                for (int j = 0; j < size; j++)
                {
                    objects.add(Utils.deserializeString(in));
                }
                map.put(key, objects);
            }
            int nAll = in.readInt();
            Map<String,String> all = new HashMap<>();
            for (int i = 0; i < nAll; i++)
            {
                String key = Utils.deserializeString(in);
                String value = Utils.deserializeString(in);
                all.put(key, value);
            }
            int bayouStabMs = in.readInt();
            return new TargetsMessage(myName, map, all, bayouStabMs);
        }
    };
    private final String myName;
    private final Map<String, List<String>> map;
    private final Map<String, String> all;
    private final int bayouStabMs;

    public TargetsMessage(String myName, Map<String, List<String>> map, Map<String, String> all, int bayouStabMs)
    {
        super(MSG_ID);
        this.myName = myName;
        this.map = map;
        this.all = all;
        this.bayouStabMs = bayouStabMs;
    }

    public String getMyName()
    {
        return myName;
    }

    public Map<String, List<String>> getMap()
    {
        return map;
    }

    public Map<String,String> getAll()
    {
        return all;
    }

    public int getBayouStabMs()
    {
        return bayouStabMs;
    }

    @Override
    public String toString()
    {
        return "TargetsMessage{" +
               "myName=" + myName +
               ", map=" + map +
               ", all=" + all +
               ", bayouStabMs=" + bayouStabMs +
               '}';
    }
}
