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
package com.ibm.watson.litelinks.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.ibm.watson.litelinks.NettyTTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TZlibTransport;
import org.apache.thrift.transport.layered.TLayeredTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;

/**
 * Utility class for extracting general transport parameters from
 * various possible {@link TTransport} impls
 */
public final class TTransportParameters {

    private static final Logger logger = LoggerFactory.getLogger(TTransportParameters.class);

    private TTransportParameters() {} // static only

    // for now just remote address
    public static Map<String, Object> getParameterMap(TTransport ttrans) {
        SocketAddress addr = null;
        SSLSession ssls = null;
        if (ttrans instanceof NettyTTransport) { // includes FramedNettyTTransport
            NettyTTransport nttrans = (NettyTTransport) ttrans;
            addr = nttrans.getRemoteAddress();
            ssls = nttrans.getSslSession();
        } else if (ttrans instanceof TSocket) {
            Socket socket = ((TSocket) ttrans).getSocket();
            addr = socket.getRemoteSocketAddress();
            if (socket instanceof SSLSocket) {
                ssls = ((SSLSocket) socket).getSession();
            }
        } else if (ttrans instanceof TNonblockingSocket) {
            try {
                addr = ((TNonblockingSocket) ttrans).getSocketChannel().getRemoteAddress();
            } catch (IOException e) {
                logger.warn("Error obtaining remote address of nonblocking socket", e);
            }
        } else {
            try {
                // unwrap TLayeredTransport, TZlibTransport
                if (ttrans instanceof TLayeredTransport)
                // this is TFramedTransport, TFastFramedTransport
                {
                    return getParameterMap(((TLayeredTransport) ttrans).getInnerTransport());
                }
                if (ttrans instanceof TZlibTransport) {
                    return getParameterMap((TTransport) tzt_trans.get(ttrans));
                }
            } catch (IllegalArgumentException | IllegalAccessException e) {
                logger.warn("Error obtaining transport params of wrapped TTransport", e);
            }
        }
        if (addr == null && ssls == null) {
            return Collections.emptyMap();
        }
        Builder<String, Object> imb = ImmutableMap.builder();
        if (addr != null) {
            imb.put(RequestListener.TP_REMOTE_ADDRESS, addr);
        }
        if (ssls != null) {
            imb.put(RequestListener.TP_SSL_SESSION, ssls);
        }
        return imb.build();
    }

    private static final Field tzt_trans;

    static {
        Field f1 = null;
        try {
            f1 = TZlibTransport.class.getDeclaredField("transport_");
            f1.setAccessible(true);
        } catch (NoSuchFieldException | SecurityException e) {
            logger.warn("Error initializing private field access for wrapped TTransports", e);
        }
        tzt_trans = f1;
    }
}
