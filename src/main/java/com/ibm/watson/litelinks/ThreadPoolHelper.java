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

import org.slf4j.MDC;

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class for creating pools with behaviour that differs
 * from vanilla {@link ThreadPoolExecutor}
 */
public class ThreadPoolHelper {

    private ThreadPoolHelper() {} // only static

    private static final RejectedExecutionHandler DEFAULT_HANDLER =
            new ThreadPoolExecutor.AbortPolicy();

    private static final RejectedExecutionHandler ENQUEUE_HANDLER = (r, executor) -> {
        try {
            executor.getQueue().put(r);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    };

    //TODO add option to bound with semaphore

    /**
     * Create new threadpool which spawns up maxThreads <i>before</i>
     * overflowing new tasks to the backing queue
     * (rather than the reverse "standard" behaviour)
     */
    @SuppressWarnings("serial")
    public static ThreadPoolExecutor newThreadPool(int coreThreads, int maxThreads,
            long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {

        // the desired behaviour is achieved via a combination of modified transferqueue
        // and custom rejected exec handler
        return new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, unit,
                new LinkedTransferQueue<Runnable>() {
                    @Override
                    public boolean offer(Runnable e) {
                        return tryTransfer(e);
                    }
                }, threadFactory, ENQUEUE_HANDLER) {
            @Override
            public void shutdown() {
                super.shutdown();
                setRejectedExecutionHandler(DEFAULT_HANDLER);
            }
        };
    }

    /**
     * Create a new thread factory which returns daemon threads
     * with the specified name format (using %d for counter substitution).
     * Created threads will have their (inherited) logging MDC cleared before running.
     */
    public static ThreadFactory threadFactory(final String nameFormat) {
        String.format(nameFormat, 0); // fail-fast for bad format
        return new ThreadFactory() {
            private final AtomicLong count = new AtomicLong();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, String.format(nameFormat, count.incrementAndGet())) {
                    @Override
                    public void run() {
                        // make sure log MDC doesn't get inherited
                        MDC.clear();
                        super.run();
                    }
                };
                t.setDaemon(true);
                return t;
            }
        };
    }
}
