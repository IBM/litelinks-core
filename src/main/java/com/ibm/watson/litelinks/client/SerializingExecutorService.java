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

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Delegates submitted tasks to the shared threadpool but ensures they
 * are executed in order and in serial
 */
public class SerializingExecutorService extends AbstractExecutorService {
    private final Executor sharedPool;
    private final Queue<Runnable> workQueue = new ConcurrentLinkedQueue<>();
    private volatile boolean scheduled;
    private volatile boolean shutdown;

    public SerializingExecutorService(Executor parentPool) {
        if (parentPool == null) {
            throw new NullPointerException();
        }
        sharedPool = parentPool;
    }

    protected void logTaskUncheckedException(Throwable t) {
        t.printStackTrace();
    }

    private final Runnable runner = new Runnable() {
        @Override
        public void run() {
            try {
                for (; ; ) {
                    Runnable next; // note "this" lock is on Runner instance
                    if((next = workQueue.poll()) == null) synchronized(this) {
                        scheduled = false;
                        notifyAll();
                        if((next = workQueue.poll()) == null) return;
                        scheduled = true;
                    }
                    try {
                        next.run();
                    } catch (RuntimeException e) {
                        logTaskUncheckedException(e);
                    }
                }
            } catch (Throwable t) {
                dispatch();
                logTaskUncheckedException(t);
                throw t;
            }
        }
    };

    @Override
    public void execute(Runnable command) {
        if (shutdown) {
            throw new RejectedExecutionException("shutdown");
        }
        workQueue.offer(command);
        if (!scheduled) {
            boolean doit = false;
            synchronized (runner) {
                if (!scheduled) {
                    scheduled = true;
                    doit = true;
                }
            }
            if (doit) dispatch();
        }
        // second shutdown check to avoid race conditions
        if (shutdown && workQueue.remove(command)) {
            throw new RejectedExecutionException("shutdown");
        }
    }

    private void dispatch() {
        boolean ok = false;
        try {
            sharedPool.execute(runner);
            ok = true;
        } finally {
            if(!ok) synchronized(runner) {
                scheduled = false; // bad situation
                runner.notifyAll();
            }
        }
    }

    public void waitUntilIdle() throws InterruptedException {
        synchronized (runner) {
            while (scheduled) {
                runner.wait();
            }
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown = true;
        List<Runnable> remaining = new LinkedList<>();
        Runnable r;
        while ((r = workQueue.poll()) != null) {
            remaining.add(r);
        }
        // not going to bother interrupting the potentially running task
        // (would need to keep a ref to it in that case)
        return remaining;
    }

    @Override
    public synchronized void shutdown() {
        shutdown = true;
    }

    @Override
    public boolean isTerminated() {
        return shutdown && workQueue.isEmpty();
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, unit), rem;
        synchronized (this) {
            while (!shutdown) {
                if ((rem = deadline - System.nanoTime()) <= 0) {
                    return false;
                }
                this.wait(rem);
            }
        }
        synchronized (runner) {
            while (scheduled) {
                if ((rem = deadline - System.nanoTime()) <= 0) {
                    return false;
                }
                runner.wait(rem);
            }
        }
        return true;
    }
}
