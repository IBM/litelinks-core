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
package io.netty.buffer;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.WrongMethodTypeException;
import java.lang.reflect.Method;

/**
 * This is in a netty package to access package-private methods.
 */
public final class ByteBufHelper {
    private static final Logger logger = LoggerFactory.getLogger(ByteBufHelper.class);

    private ByteBufHelper() {}

    public static void writeByteUnsafe(ByteBuf buf, int b) {
        if (buf instanceof AbstractByteBuf) {
            AbstractByteBuf abb = (AbstractByteBuf) buf;
            abb._setByte(abb.writerIndex++, b);
        } else {
            buf.writeByte(b);
        }
    }

    /**
     * Equivalent to {@link ByteBufUtil#reserveAndWriteUtf8(ByteBuf, CharSequence, int, int, int) but
     * does not perform bounds checks on the ByteBuf, hence unsafe.
     */
    public static int writeUtf8Unsafe(ByteBuf buf, CharSequence seq, int start, int end, int reserveBytes) {
        if (RES_AND_WRITE_UTF8_SEQ_MH != null) {
            try {
                // Use private method if possible to avoid unnecessary bounds check
                return (int) RES_AND_WRITE_UTF8_SEQ_MH.invokeExact(buf, seq, start, end, reserveBytes);
            } catch (Throwable t) {
                if (!(t instanceof WrongMethodTypeException)) {
                    Throwables.throwIfInstanceOf(t, RuntimeException.class);
                    Throwables.throwIfInstanceOf(t, Error.class);
                }
                // otherwise fall-back
            }
        }
        return ByteBufUtil.reserveAndWriteUtf8(buf, seq, start, end, reserveBytes);
    }

    // Access private method to bypass bounds checks
    private static final MethodHandle RES_AND_WRITE_UTF8_SEQ_MH;

    static {
        MethodHandle mh = null;
        try {
            Method meth = ByteBufUtil.class.getDeclaredMethod("reserveAndWriteUtf8Seq",
                    ByteBuf.class, CharSequence.class, int.class, int.class, int.class);
            meth.setAccessible(true);
            mh = MethodHandles.lookup().unreflect(meth);
        } catch (Exception e) {
            logger.warn("Unable to set up reflection for reserveAndWriteUtf8 bounds check bypass", e);
        }
        RES_AND_WRITE_UTF8_SEQ_MH = mh;
    }
}
