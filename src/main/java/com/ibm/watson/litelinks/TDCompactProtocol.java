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
import com.ibm.watson.litelinks.server.ReleaseAfterResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Optimized version of {@link TCompactProtocol} for use with
 * {@link NettyTTransport}.
 */
public class TDCompactProtocol extends TCompactProtocol {
    private static final Logger logger = LoggerFactory.getLogger(TDCompactProtocol.class);

    public static final boolean POOLED_BUFS =
            Boolean.getBoolean(LitelinksSystemPropNames.POOLED_BYTEBUFFERS);

    static {
        logger.info(LitelinksSystemPropNames.POOLED_BYTEBUFFERS + "=" + POOLED_BUFS);
    }

    //  public static final TProtocolFactory FACTORY = new TCompactProtocol.Factory();
    public static final TProtocolFactory FACTORY = tr -> tr instanceof NettyTTransport ?
            new TDCompactProtocol(tr) : new SafeBinaryTProtocol(new TCompactProtocol(tr));

    public TDCompactProtocol(TTransport trans) {
        super(trans);
    }

    protected NettyTTransport ntt() {
        return (NettyTTransport) trans_;
    }

    @Override
    public void writeBinary(ByteBuffer bin) throws TException {
        writeVarint32(bin.remaining());
        ntt().write(bin);
    }

    @Override
    public void writeString(String str) throws TException {
        int len = ByteBufUtil.utf8Bytes(str);
        writeVarint32(len);
        ntt().writeUtf8(str, len);
    }

    @Override
    public ByteBuffer readBinary() throws TException {
        int length = readVarint32();
        checkStringReadLength(length);
        if (length == 0) {
            return EMPTY_BUFFER;
        }

        if (POOLED_BUFS) {
            ByteBuf bb = ntt().readAsSingleBuf(length);
            ReleaseAfterResponse.addReleasable(bb);
            return bb.nioBuffer();
        }

        byte[] buf = new byte[length];
        trans_.readAll(buf, 0, length);
        return ByteBuffer.wrap(buf);
    }

    @Override
    public String readString() throws TException {
        int length = readVarint32();
        checkStringReadLength(length);
        if (length == 0) {
            return "";
        }
        ByteBuf buf = ntt().readAll(length);
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
    protected void writeByteDirect(int b) throws TException {
        ntt().writeByte(b);
    }

    @Override
    protected void writeByteDirect(byte b) throws TException {
        ntt().writeByte(b);
    }
}
