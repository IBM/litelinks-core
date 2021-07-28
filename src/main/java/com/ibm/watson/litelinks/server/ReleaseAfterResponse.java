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

import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * This class can be used to allow server logic to declare resources
 * that should be closed/released only once the response has completed
 * sending. It is intended for advanced optimizations, not general use.
 * <p>
 * It should also be considered experimental and subject to change.
 */
@SuppressWarnings("serial")
public class ReleaseAfterResponse extends LinkedList<Object> implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ReleaseAfterResponse.class);

    private static final FastThreadLocal<ReleaseAfterResponse> tlSet
            = new FastThreadLocal<ReleaseAfterResponse>() {
        @Override
        protected void onRemoval(ReleaseAfterResponse value) {
            if (value != null) {
                value.finished();
            }
        }
    };

    private static class FinalizingReleaseAfterResponse extends ReleaseAfterResponse {
        @Override
        protected void finalize() throws Throwable {
            try {
                super.finalize();
            } finally {
                finished();
            }
        }
    }

    private static ReleaseAfterResponse instanceForThread() {
        return FastThreadLocalThread.willCleanupFastThreadLocals(Thread.currentThread())
                ? new ReleaseAfterResponse() : new FinalizingReleaseAfterResponse();
    }

    private ReleaseAfterResponse() {}

    private static void add0(Object obj) {
        ReleaseAfterResponse rar = tlSet.get();
        if (rar == null) {
            tlSet.set(rar = instanceForThread());
        }
        rar.add(obj);
    }

    public static void addCloseable(AutoCloseable obj) {
        add0(obj);
    }

    public static void addReleasable(ReferenceCounted obj) {
        add0(obj);
    }

    /**
     * @return an {@link AutoCloseable} which must be closed
     * to free this thread's releaseable resources, or
     * null if there are none. Subsequently calling  {@link #releaseAll()}
     * from this thread <b>won't</b> release the resources (unless
     * further have since been added).
     */
    public static AutoCloseable takeOwnership() {
        final ReleaseAfterResponse rar = tlSet.get();
        if (rar == null) {
            return null;
        }
        // can't call remove() here since we don't want onRemoval
        // to be invoked
        tlSet.set(null);
        return rar;
    }

    public static void releaseAll() {
        ReleaseAfterResponse rar = tlSet.get();
        if (rar == null) {
            return;
        }
        rar.close();
        tlSet.remove();
    }

    public static boolean releaseRequired() {
        return tlSet.get() != null;
    }

    @Override
    public void close() {
        if (!isEmpty()) for (Iterator<Object> it = iterator(); it.hasNext();) try {
            Object o = it.next();
            if (o instanceof ReferenceCounted) ((ReferenceCounted) o).release();
            else if (o instanceof AutoCloseable) ((AutoCloseable) o).close();
            it.remove();
        } catch (Exception e) {
            logger.warn("Exception while releasing resource", e);
        }
    }

    protected final void finished() {
        if (!isEmpty()) {
            logger.warn("Releasing " + size() + " resources after thread completion which"
                        + " should have been released explicitly");
            close(); // last resort
        }
    }
}
