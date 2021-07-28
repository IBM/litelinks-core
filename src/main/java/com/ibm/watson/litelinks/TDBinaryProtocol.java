/*
 * Copyright 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ibm.watson.litelinks;

import com.ibm.watson.litelinks.LitelinksTProtoExtension.SafeBinaryTProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Optimized version of {@link TBinaryProtocol} for use with
 * {@link NettyTTransport}.
 */
public class TDBinaryProtocol extends TBinaryProtocol {

    public static final TProtocolFactory FACTORY = tr -> tr instanceof NettyTTransport?
            new TDBinaryProtocol(tr) : new SafeBinaryTProtocol(new TBinaryProtocol(tr));

    public TDBinaryProtocol(TTransport trans) {
        super(trans);
    }

    protected NettyTTransport ntt() {
        return (NettyTTransport) trans_;
    }

    @Override
    public void writeBinary(ByteBuffer bin) throws TException {
        writeI32(bin.remaining());
        ntt().write(bin);
    }

    @Override
    public void writeString(String str) throws TException {
        int len = ByteBufUtil.utf8Bytes(str);
        writeI32(len);
        ntt().writeUtf8(str, len);
    }

    protected static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);

    @Override
    public ByteBuffer readBinary() throws TException {
        int length = readI32();
        if (length == 0) {
            return EMPTY_BUFFER;
        }

        byte[] buf = new byte[length];
        trans_.readAll(buf, 0, length);
        return ByteBuffer.wrap(buf);
    }

    @Override
    public String readStringBody(int size) throws TException {
        if (size == 0) {
            return "";
        }
        ByteBuf buf = ntt().readAll(size);
        try {
            return buf.toString(StandardCharsets.UTF_8);
        } finally {
            buf.release();
        }
    }

    @Override
    public byte readByte() throws TException {
        return ntt().read();
    }

    @Override
    public void writeByte(byte b) throws TException {
        ntt().writeByte(b);
    }

}
