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

import com.ibm.watson.litelinks.InvalidThriftClassException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.reflect.Constructor;

/**
 * This class is used to convert an arbitrary Thrift service implementation to
 * a {@link ThriftService} class for litelinks deployment.
 * <p>
 * The class must declare a no-arg (or default) constructor. If it implements
 * {@link Closeable}, it's {@link Closeable#close()} method will be invoked
 * during shutdown.
 */
public class AdapterThriftService extends ThriftService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultThriftServer.class);

    private static final String THRIFT_CLIENT_IFACE_NAME = "Iface";

    private final String handlerClassName;
    private final Constructor<? extends TProcessor> tprocConstr;
    private final Constructor<?> handlerConstr;
    private Object handler;

    /**
     * @param handlerClassName
     * @throws ClassNotFoundException      if the class is not found
     * @throws InvalidThriftClassException if the class does not implement a thrift-generated
     *                                     service interface (Iface), or doesn't have a no-arg constructor
     */
    public AdapterThriftService(String handlerClassName)
            throws ClassNotFoundException, InvalidThriftClassException {
        this(Class.forName(handlerClassName));
    }

    /**
     * @param handlerClass
     * @throws InvalidThriftClassException if the class does not implement a thrift-generated
     *                                     service interface (Iface), or doesn't have a no-arg constructor
     */
    public AdapterThriftService(Class<?> handlerClass) throws InvalidThriftClassException {
		this.handlerClassName = handlerClass.getName();
        try {
			this.handlerConstr = handlerClass.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new InvalidThriftClassException("Handler class must have "
                                                  + "no-arg constructor: " + handlerClassName);
        }
        final Constructor<? extends TProcessor> tprocConstr = getTProcConstructor(handlerClass);
        if (tprocConstr == null) {
            throw new InvalidThriftClassException(handlerClassName);
        }
        this.tprocConstr = tprocConstr;
    }

    @Override
    protected TProcessor initialize() throws Exception {
        handler = handlerConstr.newInstance();
        return tprocConstr.newInstance(handler);
    }

    @Override
    protected void shutdown() {
        Object hinst = handler;
        if (hinst instanceof Closeable) {
            try {
                ((Closeable) hinst).close();
            } catch (Exception e) {
                logger.warn("Exception while shutting down service impl: " + handlerClassName, e);
            }
        }
    }

    protected static Constructor<? extends TProcessor> getTProcConstructor(Class<?> clz) {
        if (clz == null) {
            return null;
        }
        for (Class<?> iface : clz.getInterfaces()) {
            if (!iface.getSimpleName().equals(THRIFT_CLIENT_IFACE_NAME)) {
                continue;
            }
            Constructor<? extends TProcessor> constr = getTProcConstructorFromIface(iface);
            if (constr != null) {
                return constr;
            }
        }
        // need to check superclasses to find all implemented interfaces
        return getTProcConstructor(clz.getSuperclass());
    }

    protected static Class<?> getIfaceFromSvcClass(Class<?> svcClass) {
        for (Class<?> inner : svcClass.getDeclaredClasses()) {
            if (inner.getSimpleName().equals(THRIFT_CLIENT_IFACE_NAME)) {
                return inner;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    protected static Constructor<? extends TProcessor> getTProcConstructorFromIface(Class<?> iface) {
        Class<?> topSvcClass = iface.getDeclaringClass();
        if (topSvcClass != null) {
            Class<?>[] svcClasses = topSvcClass.getDeclaredClasses();
            if (svcClasses != null) {
                for (Class<?> sc : svcClasses) {
                    if (TProcessor.class.isAssignableFrom(sc)) {
                        try {
                            return (Constructor<? extends TProcessor>) sc.getConstructor(iface);
                        } catch (NoSuchMethodException e) {
                            continue;
                        }
                    }
                }
            }
        }
        return null;
    }

}

