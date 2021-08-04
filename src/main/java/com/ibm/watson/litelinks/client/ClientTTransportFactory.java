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
package com.ibm.watson.litelinks.client;

import com.ibm.watson.litelinks.FramedNettyTTransport;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.NettyTTransport;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.net.InetSocketAddress;

/**
 *
 */
public abstract class ClientTTransportFactory {

    /**
     * @param hostname
     * @param port
     * @param timeoutMillis
     * @param framed
     * @param sslContext
     * @throws TTransportException
     */
    public abstract TTransport openNewTransport(String hostname, int port,
            long timeoutMillis, boolean framed, SslContext sslContext) throws TTransportException;

    public TTransport openNewTransport(String hostname, int port,
            long timeoutMillis, boolean framed) throws TTransportException {
        return openNewTransport(hostname, port, timeoutMillis, framed, null);
    }

    /**
     * SOCKET client transport can only be used with JDK TLS impl, not OpenSSL.
     * See {@link LitelinksSystemPropNames#USE_JDK_TLS}.
     */
    public static final ClientTTransportFactory SOCKET = new ClientTTransportFactory() {
        @Override
        public TTransport openNewTransport(String hostname, int port,
                long timeoutMillis, boolean framed, SslContext sslContext) throws TTransportException {

            TSocket socket = null;
            try {
                if (sslContext == null) {
                    socket = new TSocket(TConfiguration.DEFAULT, hostname, port, (int) timeoutMillis);
                } else {
                    try {
                        SSLContext jdkContext = ((JdkSslContext) sslContext).context();
                        SSLSocket sock = (SSLSocket) jdkContext.getSocketFactory().createSocket(hostname, port);
                        sock.setUseClientMode(true);
                        sock.setSoTimeout((int) timeoutMillis);
                        socket = new TSocket(sock);
                    } catch (Exception e) {
                        throw new TTransportException("Could not connect to " + hostname + " on port " + port, e);
                    }
                }
                TTransport transport = framed? new TFramedTransport(socket) : socket;
                if (!transport.isOpen()) {
                    transport.open();
                }
                socket.setTimeout(0); // set to indefinite initially
                socket = null;
                return transport;
            } finally {
                if (socket!=null) try { socket.close(); } catch(Throwable t) {}
            }
        }
    };

    public static final ClientTTransportFactory NETTY = new ClientTTransportFactory() {
        @Override
        public TTransport openNewTransport(String hostname, int port,
                long timeoutMillis, boolean framed, SslContext sslContext) throws TTransportException {
            ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;
            //TODO *maybe* cache these address objects (but if so only
            // short-lived since we may want to refresh DNS)
            final InetSocketAddress addr = new InetSocketAddress(hostname, port);
            final TTransport transport = framed?
                    new FramedNettyTTransport(addr, timeoutMillis, alloc, sslContext) :
                    new NettyTTransport(addr, timeoutMillis, alloc, sslContext);
            transport.open();
            return transport;
        }
    };

}
