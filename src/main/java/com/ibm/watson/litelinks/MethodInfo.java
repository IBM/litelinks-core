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

import com.google.common.base.Splitter;
import com.ibm.watson.litelinks.server.Idempotent;
import com.ibm.watson.litelinks.server.InstanceFailures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Indicates server-defined configuration associated with a service method,
 * for use by clients when remotely invoking this method.
 * <p>
 * Unless this info needs to be determined dynamically per instance, it's
 * recommended to instead use the {@link Idempotent} and/or {@link InstanceFailures}
 * annotations on service implementation methods.
 */
public class MethodInfo {

    private static final Logger logger = LoggerFactory.getLogger(MethodInfo.class);

    /**
     * A special method name used to indicate default method configuration,
     * used for all methods which don't have their own MethodInfo or equivalent
     * annotations.
     */
    public static final String DEFAULT = "*DEFAULT*";

    public static final MethodInfo DEFAULT_MI = new MethodInfo(false, null);
    public static final MethodInfo IDEMPOTENT = new MethodInfo(true, null);

    private final boolean idempotent;
    private final Set<Class<? extends Exception>> instanceFailureExceptions;
//	final long defaultTimeout; //TODO possible future addition

    public static Builder builder() {
        return new Builder();
    }

    protected MethodInfo(boolean idempotent, Set<Class<? extends Exception>> instanceFailureExceptions) {
        this.idempotent = idempotent;
        this.instanceFailureExceptions = instanceFailureExceptions;
    }

    public boolean isIdempotent() {
        return idempotent;
    }

    public Set<Class<? extends Exception>> instanceFailureExceptions() {
        return instanceFailureExceptions;
    }

    //TODO maybe ser/deser logic outside
    // only public access because used from .server and .client packages

    public String serialize() {
        String idp = idempotent ? "idp=t" : "idp=f";
        if (instanceFailureExceptions == null || instanceFailureExceptions.isEmpty()) {
            return idp;
        }
        StringBuilder sb = new StringBuilder(idp).append(";ifexs=");
        boolean first = true;
        for (Class<? extends Exception> ec : instanceFailureExceptions) {
            if (ec != null) {
                if (first) {
                    first = false;
                } else {
                    sb.append(',');
                }
                sb.append(ec.getName());
            }
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    public static MethodInfo deserialize(String str) {
        if (str == null || str.isEmpty()) {
            return null;
        }
        Builder bld = new Builder();
        for (String part : Splitter.on(';').split(str)) {
            String[] kv = part.split("=");
            if (kv.length != 2) {

            } else {
                switch (kv[0]) {
                case "idp":
                    bld.setIdempotent(kv[1].length() > 0 && kv[1].charAt(0) == 't');
                    break;
                case "ifexs":
                    Set<Class<? extends Exception>> set = new HashSet<>();
                    for (String exName : Splitter.on(',').split(kv[1])) {
                        try {
                            Class<?> clz = Class.forName(exName);
                            if (!Exception.class.isAssignableFrom(clz)) {
                                throw new ClassCastException();
                            }
                            set.add((Class<? extends Exception>) clz);
                        } catch (ClassNotFoundException | ClassCastException e) {
                            logger.warn("Invalid or unrecognized exception type when parsing MethodInfo: " + exName);
                        }
                    }
                    if (!set.isEmpty()) {
                        bld.setInstanceFailureExceptions(set);
                    }
                    break;
                }
            }
        }
        return bld.build();
    }

    /**
     * Used to build immutable {@link MethodInfo} objects
     */
    public static class Builder {
        private boolean idempotent;
        private Set<Class<? extends Exception>> instanceFailureExceptions;

        /**
         * This flag indicates that the corresponding method is idempotent,
         * that is successive invocations with the same input will not
         * have any effect, and will return an identical response. This
         * includes but is not limited to "read-only" operations. It is
         * used by clients to know when to safely retry (typically against
         * a different service instance) in the case of failures.
         * <p>
         * This can also be indicated via use of the {@link Idempotent}
         * annotation on the service implementation of the method.
         *
         * @param idempotent true if the method is idempotent, false otherwise
         */
        public Builder setIdempotent(boolean idempotent) {
            this.idempotent = idempotent;
            return this;
        }

        /**
         * A list of {@link Exception} types which when thrown by this method
         * indicate an "internal" problem with the source service instance.
         * Clients use this information in "automatic" failure handling decisions.
         * In particular, requests will be retried if the method is also idempotent,
         * and further requests won't be sent to the problem instance for some
         * period (if others are available).
         * <p>
         * This can also be indicated via use of the {@link InstanceFailures}
         * annotation on the service implementation of the method.
         *
         * @param instanceFailureExceptions
         */
        public Builder setInstanceFailureExceptions(Set<Class<? extends Exception>> instanceFailureExceptions) {
            this.instanceFailureExceptions = instanceFailureExceptions;
            return this;
        }

        public MethodInfo build() {
            if (instanceFailureExceptions != null && instanceFailureExceptions.isEmpty()) {
                instanceFailureExceptions = null;
            }
            if (instanceFailureExceptions == null) {
                return idempotent ? IDEMPOTENT : DEFAULT_MI;
            }
            return new MethodInfo(idempotent, instanceFailureExceptions);
        }
    }
}
