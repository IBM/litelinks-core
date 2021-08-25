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

import com.ibm.watson.litelinks.client.ThriftClientBuilder;
import io.netty.util.concurrent.FastThreadLocal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.lang.System.nanoTime;

/**
 * Thread-local context to be propagated across litelinks service interfaces.
 * <p>
 * When a particular client is used, any entries set on the current thread
 * via this class will be merged with those set via the {@link
 * ThriftClientBuilder#withContext(Map)} and/or {@link ThriftClientBuilder
 * #withContextEntry(String, String)}, with those set via this class taking
 * precedence upon key collisions.
 * <p>
 * It's also possible to implicitly propagate String-based logging MDC entries
 * by setting the following JVM arg on the client side:
 * -Dlitelinks.threadcontexts=log_mdc
 * <p>
 * Note that in versions of litelinks prior to 1.1.0, this JVM arg needed to
 * be set with "custom" as one of the comma separated "types" for this custom
 * ThreadContext to be sent, but that is no longer necessary (it will always
 * be sent if context parameters are present).
 */
public final class ThreadContext {

    private ThreadContext() {}

    /**
     * General constant which can be used as a key in the context map for
     * authorization purposes
     */
    public static final String AUTH_TOKEN = "AUTH_TOKEN";

    /**
     * General constant which can be used to indicate that authorization is
     * not necessary "these aren't the droids you're looking for"
     */
    public static final String SKIP_AUTH = "SKIP_AUTH";

    private static final FastThreadLocal<ThreadContext> context
            = new FastThreadLocal<ThreadContext>() {
        @Override
        protected ThreadContext initialValue() {
            return new ThreadContext();
        }
    };

    private static boolean inUse;

    private boolean userSupplied;
    private Map<String, String> contextMap;

    private static final long BASE_NANOS = nanoTime();

    // relative to BASE_NANOS, or -1 if no deadline set
    private long deadlineNanos = -1L;

    /**
     * Get the current thread's context map
     */
    public static Map<String, String> getCurrentContext() {
        return inUse ? context.get().contextMap : null;
    }

    public static String getContextEntry(String key) {
        Map<String, String> curMap = getCurrentContext();
        return curMap == null ? null : curMap.get(key);
    }

    /**
     * @return 0 if expired, -1 if no deadline set
     */
    public static long nanosUntilDeadline() {
        if (!inUse) {
            return -1L;
        }
        long d = context.get().deadlineNanos;
        if (d == -1L) {
            return -1L;
        }
        d = BASE_NANOS + d - nanoTime();
        return d >= 0 ? d : (d < Long.MIN_VALUE / 2 ? Long.MAX_VALUE : 0L);
    }

    public static boolean isDeadlineExpired() {
        return nanosUntilDeadline() == 0L; // (-1 => no deadline)
    }

    /**
     * Set the current thread's context map. Will overwrite
     * any parameters previously set on this thread via the
     * {@link #addContextEntry(String, String)} method
     *
     * @param map
     */
    public static void setCurrentContext(Map<String, String> map) {
        context.get().thisSetCurrentContext(map);
        inUse = true;
    }

    /**
     * @param map
     * @param nanosUntilDeadline -1 for no deadline, or otherwise &gt;= 0
     */
    public static void setCurrentContext(Map<String, String> map,
            long nanosUntilDeadline) {
        context.get().thisSetCurrentContext(map, nanosUntilDeadline);
        inUse = true;
    }

    /**
     * @param nanos -1 to unset deadline, or otherwise &gt;= 0
     */
    public static void setDeadlineAfter(long nanos) {
        context.get().thisSetDeadlineAfter(nanos);
        inUse = true;
    }

    /**
     * @param duration
     * @param unit
     */
    public static void setDeadlineAfter(long duration, TimeUnit unit) {
        setDeadlineAfter(TimeUnit.NANOSECONDS.convert(duration, unit));
    }

    public static void removeDeadline() {
        if (inUse) {
            setDeadlineAfter(-1L);
        }
    }

    private void thisSetCurrentContext(Map<String, String> map) {
        contextMap = map;
        userSupplied = map != null;
    }

    private void thisSetCurrentContext(Map<String, String> map,
            long nanosUntilDeadline) {
        thisSetDeadlineAfter(nanosUntilDeadline);
        contextMap = map;
        userSupplied = map != null;
    }

    private void thisSetDeadlineAfter(long nanos) {
        deadlineNanos = nanos == -1L ? -1L
                : nanoTime() + Math.max(0L, nanos) - BASE_NANOS;
    }

    /**
     * Internal use only
     */
    public static void revertCurrentContext(Map<String, String> map) {
        context.get().contextMap = map;
    }

    /**
     * Adds a key/value pair to the current thread's context map.
     * Note that if the {@link #setCurrentContext(Map)} has previously
     * been used on this thread to set an explicit map, the original
     * map will no longer be used directly and this parameter
     * will be added to a new <b>copy</b> of that map.
     *
     * @param key
     * @param value
     */
    public static void addContextEntry(String key, String value) {
        context.get().thisAddContextEntry(key, value);
        inUse = true;
    }

    private void thisAddContextEntry(String key, String value) {
        if (userSupplied || !(contextMap instanceof HashMap)) {
            if (contextMap == null) {
                contextMap = Collections.singletonMap(key, value);
                return;
            }
            if (Objects.equals(value, contextMap.get(key))) {
                return;
            }
            contextMap = new HashMap<>(contextMap);
            //TODO log warning if userSupplied == true
        }
        contextMap.put(key, value);
    }

    /**
     * Remove the current thread's context map <b>and deadline</b>; no extra data
     * will be included in subsequent litelinks requests from this thread
     */
    public static void removeCurrentContext() {
        context.remove();
    }

}
