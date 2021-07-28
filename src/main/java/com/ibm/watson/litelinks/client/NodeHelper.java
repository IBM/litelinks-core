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

import com.ibm.watson.kvutils.OrderedShutdownHooks;
import org.apache.thrift.async.AsyncMethodCallback;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Special-purpose helper for node.js integration via node-java library.
 * Not intended for general purpose use!
 */
public class NodeHelper {

    private NodeHelper() {}

    static class ReqIdGenerator {

        // since java Thread.getId() may be reused
        private static final AtomicInteger threadIds = new AtomicInteger();

        private static final ThreadLocal<ReqIdGenerator> forThread = new ThreadLocal<ReqIdGenerator>() {
            @Override
            protected ReqIdGenerator initialValue() {
                return new ReqIdGenerator();
            }
        };

        public static String nextId() {
            return forThread.get().next();
        }

        private ReqIdGenerator() {}

        // these accessed only by owning thread
        private final StringBuilder tidBld = new StringBuilder(
                Integer.toHexString(threadIds.incrementAndGet())).append('-');
        private final int prefixLen = tidBld.length();
        private long rid;

        private String next() {
            tidBld.setLength(prefixLen);
            return tidBld.append(Long.toHexString(++rid)).toString();
        }
    }

    private static final ResultCallback<?> POISON = new ResultCallback<>();

    static final BlockingQueue<ResultCallback<?>> resultQueue = new LinkedBlockingQueue<>();

    public static class ResultCallback<T> implements AsyncMethodCallback<T> {
        public final String requestId = ReqIdGenerator.nextId();
        // One of the below fields is set upon completion prior to enqueue.
        // These should be private; they are public to allow direct access as
        // properties via node-java
        public Exception error;
        public T value;

        ResultCallback() {}

        @Override
        public void onComplete(T response) {
            value = response;
            resultQueue.add(this);
        }

        @Override
        public void onError(Exception exception) {
            error = exception;
            resultQueue.add(this);
        }

        public String getRequestId() {
            return requestId;
        }

        public Exception getError() {
            return error;
        }

        public T getValue() {
            return value;
        }
    }

    /**
     * @return result callback object containing requestId
     */
    public static <T> ResultCallback<T> newResultCallback() {
        return new ResultCallback<T>();
    }

    /**
     * Blocks indefinitely
     *
     * @return
     * @throws InterruptedException
     */
    public static ResultCallback<?> nextResult() throws InterruptedException {
        ResultCallback<?> result = resultQueue.take();
        if (result != POISON) {
            return result;
        }
        resultQueue.add(POISON);
        throw new CancellationException("shutdown called");
    }

    /**
     * Have waiting threads wake up with a CancelledException
     */
    public static void shutdown() {
        // racy is ok
        if (resultQueue.peek() != POISON) {
            resultQueue.add(POISON);
        }
    }

    static {
        OrderedShutdownHooks.addHook(10, NodeHelper::shutdown);
    }
}
