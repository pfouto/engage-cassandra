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

package pfouto;

import com.google.common.base.Charsets;

import io.netty.buffer.ByteBuf;

public class Utils
{

    public static void serializeString(String s, ByteBuf buffer){
        byte[] byteArray = s.getBytes(Charsets.UTF_8);
        buffer.writeInt(byteArray.length);
        buffer.writeBytes(byteArray);
    }

    public static String deserializeString(ByteBuf buffer){
        int size = buffer.readInt();
        byte[] stringBytes = new byte[size];
        buffer.readBytes(stringBytes);
        return new String(stringBytes, Charsets.UTF_8);
    }
}
