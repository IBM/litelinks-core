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

import org.apache.thrift.transport.TTransportException;

/**
 * This class is used in preference to thrift's TTransportException superclass.
 * It prints the type as the message, which otherwise can't be seen in stacktraces.
 * <p>
 * It also includes a flag to indicate whether the exception happened before any
 * data was sent for the current API invocation, meaning it's definitely safe
 * to retry it.
 */
public class WTTransportException extends TTransportException {

    private static final long serialVersionUID = 5858853559346351701L;

    static final String[] typeStrings = {
            "UNKNOWN", "NOT_OPEN", "ALREADY_OPEN", "TIMED_OUT", "END_OF_FILE"
    };

    private final boolean beforeWriting;

    public WTTransportException() {
        this(false);
    }

    public WTTransportException(boolean beforeWriting) {
        this.beforeWriting = beforeWriting;
    }

    public WTTransportException(int type) {
        this(type, false);
    }

    public WTTransportException(int type, boolean beforeWriting) {
        super(type);
        this.beforeWriting = beforeWriting;
    }

    public WTTransportException(int type, String message) {
        this(type, message, false);
    }

    public WTTransportException(int type, String message, boolean beforeWriting) {
        super(type, message);
        this.beforeWriting = beforeWriting;
    }

    public WTTransportException(String message) {
        this(message, false);
    }

    public WTTransportException(String message, boolean beforeWriting) {
        super(message);
        this.beforeWriting = beforeWriting;
    }

    public WTTransportException(int type, Throwable cause) {
        this(type, cause, false);
    }

    public WTTransportException(Throwable cause) {
        this(cause, false);
    }

    public WTTransportException(Throwable cause, boolean beforeWriting) {
        super(cause);
        this.beforeWriting = beforeWriting;
    }

    public WTTransportException(String message, Throwable cause) {
        this(message, cause, false);
    }

    public WTTransportException(String message, Throwable cause, boolean beforeWriting) {
        super(message, cause);
        this.beforeWriting = beforeWriting;
    }

    public WTTransportException(int type, Throwable cause, boolean beforeWriting) {
        super(type, cause);
        this.beforeWriting = beforeWriting;
    }

    public WTTransportException(int type, String message, Throwable cause) {
        super(type, message, cause);
        this.beforeWriting = false;
    }

    public boolean isBeforeWriting() {
        return beforeWriting;
    }

    @Override
    public String getMessage() {
        String m = super.getMessage(), ts = getTypeString();
        return m != null? m + " (" + ts + ")" : ts;
    }

    public String getTypeString() {
        int t = type_;
        return typeStrings[t > 0 && t < typeStrings.length? t : 0];
    }
}
