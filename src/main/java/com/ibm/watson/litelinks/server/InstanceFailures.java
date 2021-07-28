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

import com.ibm.watson.litelinks.MethodInfo;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates exception types which should be considered as internal
 * instance failures. See
 * {@link MethodInfo.Builder#setInstanceFailureExceptions(java.util.Set)}
 * for more info.
 *
 * @see MethodInfo.Builder#setInstanceFailureExceptions(java.util.Set)
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface InstanceFailures {
    /**
     * @return an array Exception types thrown by this method
     * which should result in clients considering the service
     * instance to be failed, retrying another service
     * instance if available/applicable.
     * @see MethodInfo.Builder#setInstanceFailureExceptions(java.util.Set)
     */
    Class<? extends Exception>[] value();
}
