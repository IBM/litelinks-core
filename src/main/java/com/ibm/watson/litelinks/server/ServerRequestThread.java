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

import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 *
 */
public class ServerRequestThread extends FastThreadLocalThread {
    private static final Logger logger = LoggerFactory.getLogger(ServerRequestThread.class);

    public final long MIN_TIME_BEFORE_INTERRUPT_LOG_MS
            = Long.getLong(LitelinksSystemPropNames.MIN_TIME_BEFORE_INTERRUPT_LOG, 4_000L);

    private long reqStartTime;
    private String reqMethodName;

    public ServerRequestThread(Runnable r) {
        super(r);
    }

    public ServerRequestThread(Runnable r, String name) {
        super(r, name);
    }

    @Override
    public void run() {
        MDC.clear(); // just in case - clear any inherited MDC
        super.run();
    }

    @Override
    public void interrupt() {
        if (this != Thread.currentThread()) {
            // don't log if server thread interrupts itself
            long runningTimeMillis = runningTimeMillis();
            if (logInterrupt(runningTimeMillis)) {
                //TODO possibly later also stash request thread's MDC
                // as another field and use it for this log message
                Throwable t = new Throwable("stacktrace at time of interrupt");
                t.setStackTrace(getStackTrace());
                String method = reqMethodName;
                logger.info("Interrupting server thread " + getName() +
                            (method != null ? " processing method " + method : "") +
                            " after " + runningTimeMillis + "ms", t);
            }
        }
        super.interrupt();
    }

    void reset() {
        reqStartTime = 0L;
        reqMethodName = null;
    }

    void startRequest(long nanoTime) {
        reqStartTime = nanoTime != 0L ? nanoTime : 1L;
    }

    // public because called from LitelinksTProtoExtension
    public void setMethodName(String name) {
        reqMethodName = name;
    }

    String getMethodName() {
        return reqMethodName;
    }

    long runningTimeMillis() {
        return reqStartTime == 0L ? 0L : NettyTServer.msSince(reqStartTime);
    }

    protected boolean logInterrupt(long runningTimeMillis) {
        return runningTimeMillis > MIN_TIME_BEFORE_INTERRUPT_LOG_MS;
    }
}
