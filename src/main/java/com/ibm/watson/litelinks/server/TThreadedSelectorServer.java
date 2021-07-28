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

import io.netty.handler.ssl.SslContext;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;

/**
 * Small wrapper around {@link org.apache.thrift.server.TThreadedSelectorServer} which
 * encapsulates creation of the {@link TNonblockingServerSocket} from a port number,
 * and provides the {@link #getListeningAddress()} method for obtaining the (possibly
 * dynamically-assigned) listening port once the server is running.
 */
public class TThreadedSelectorServer extends org.apache.thrift.server.TThreadedSelectorServer
        implements ListeningService {

    protected final InetSocketAddress bindAddr;
    protected final SslContext sslContext;

    public static class Args extends org.apache.thrift.server.TThreadedSelectorServer.Args {
        public final InetSocketAddress bindAddr;
        public /*final*/ SslContext sslContext;

        public Args(InetSocketAddress bindAddr) {
            this(bindAddr, null);
        }

        public Args(InetSocketAddress bindAddr, SslContext sslContext) {
            super(null);
            this.bindAddr = bindAddr;
            this.sslContext = sslContext;
        }

        public void sslContext(SslContext sslContext) {
            this.sslContext = sslContext;
        }
    }

    public TThreadedSelectorServer(Args args) {
        super(args);
        bindAddr = args.bindAddr;
        sslContext = args.sslContext;
    }

    @Override
    public void serve() {
        if (sslContext != null) {
            throw new RuntimeException(TThreadedSelectorServer.class.getSimpleName()
                                       + " does not support SSL");
        }
        try {
            serverTransport_ = new TNonblockingServerSocket(bindAddr);
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
        super.serve();
    }

    @Override
    public SocketAddress getListeningAddress() {
        TServerTransport stransport = serverTransport_;
        return stransport instanceof TNonblockingServerSocket?
                getSSAddress((TNonblockingServerSocket) stransport) : bindAddr;
    }

    // this is needed because the thrift ServerSocket object is private/inaccessible

    private static SocketAddress getSSAddress(TNonblockingServerSocket tnss) {
        try {
            return ((ServerSocket) ssoc.get(tnss)).getLocalSocketAddress();
        } catch (IllegalAccessException e) {
            return null;
        }
    }

    private static final Field ssoc;

    static {
        try {
            ssoc = TNonblockingServerSocket.class.getDeclaredField("serverSocket_");
            ssoc.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

}
