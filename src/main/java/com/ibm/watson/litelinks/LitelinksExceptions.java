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

import com.ibm.watson.litelinks.client.ServiceUnavailableException;

import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;

/**
 * Utility class to help with interpreting exceptions thrown by
 * litelinks client methods
 */
public class LitelinksExceptions {

    private LitelinksExceptions() {} // static only

    private static final StackTraceElement[] EMPTY_STACK = new StackTraceElement[0];

    /**
     * @param e
     * @return true if it's known for sure that this RPC
     * failed prior to any data being sent - i.e.
     * no state will have changed on the service side
     * and it's safe to retry indiscriminately
     */
    public static boolean noDataSent(Throwable e) {
        return e != null && (e.getCause() instanceof ServiceUnavailableException
            || e instanceof WTTransportException && ((WTTransportException) e).isBeforeWriting());
    }

    /**
     * @param t
     * @return true if causal chain contains an {@link InterruptedException}
     */
    public static boolean isInterruption(Throwable t) {
        while (t != null) {
            if (t instanceof InterruptedException ||
                t instanceof InterruptedIOException ||
                t instanceof ClosedByInterruptException) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /**
     * @param t
     * @return t, with stacktrace removed
     */
    public static <T extends Throwable> T eraseStackTrace(T t) {
        t.setStackTrace(EMPTY_STACK);
        return t;
    }
}
