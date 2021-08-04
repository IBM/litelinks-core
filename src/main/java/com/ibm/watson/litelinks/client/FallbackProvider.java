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

import org.apache.thrift.TException;

/**
 * Used to specify fallback logic for a particular method
 *
 * @param <T> corresponding method return type
 */
@FunctionalInterface
public interface FallbackProvider<T> {

    /**
     * Encapsulates information about the failure and
     * originating method invocation
     */
    interface FailureInformation {

        //TODO add descriptions
        enum Cause {
            TIMEOUT, CONN_FAILURE, LOCAL_FAILURE, TOO_MANY_CONNS, APP_EXCEPTION, UNAVAILABLE, UNKNOWN
        }

        /**
         * @return the arguments used for the failed method invocation
         */
        Object[] getOriginalArgs();

        /**
         * <b>NOTE:</b> This exception might not be a {@link TException}. If it is to be re-thrown
         * it must first be wrapped in a {@link TException} if it's not a {@link TException} or
         * a {@link RuntimeException}.
         *
         * @return the exception which caused the failure, or null
         */
        Throwable getException();

        /**
         * @return the cause of the failure
         */
        Cause getCause();
    }


    /**
     * @param failInfo information about the cause of the failure
     * @return the fallback value
     * @throws TException any exception thrown will be re-thrown from
     *                    the originally invoked method
     */
    T getFallback(FailureInformation failInfo) throws TException;

}
