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
            out.writeInt(msg.getMap().size());
            msg.getMap().forEach((k, v) -> {
                Utils.serializeString(k, out);
                out.writeInt(v.size());
                v.forEach(h -> Utils.serializeString(h, out));
            });
            out.writeInt(msg.getAll().size());
            msg.getAll().forEach(h -> Utils.serializeString(h, out));
        }

        @Override
        public TargetsMessage deserialize(ByteBuf in) throws IOException
        {
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
            Set<String> all = new HashSet<>();
            for (int i = 0; i < nAll; i++)
            {
                all.add(Utils.deserializeString(in));
            }
            return new TargetsMessage(map, all);
        }
    };
    private final Map<String, List<String>> map;
    private final Set<String> all;

    public TargetsMessage(Map<String, List<String>> map, Set<String> all)
    {
        super(MSG_ID);
        this.map = map;
        this.all = all;
    }

    public Map<String, List<String>> getMap()
    {
        return map;
    }

    public Set<String> getAll()
    {
        return all;
    }

    @Override
    public String toString()
    {
        return "TargetsMessage{" +
               "map=" + map +
               ", all=" + all +
               '}';
    }
}
