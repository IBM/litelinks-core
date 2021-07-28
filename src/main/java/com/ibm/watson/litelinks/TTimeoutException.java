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

public class TTimeoutException extends WTTransportException {

    private static final long serialVersionUID = 912245111983188771L;

    private boolean beforeReading;

    public TTimeoutException() {
        super(TIMED_OUT);
    }

    public TTimeoutException(boolean beforeWriting, boolean beforeReading) {
        super(TIMED_OUT, beforeWriting);
        this.beforeReading = beforeReading;
    }

    public TTimeoutException(String message) {
        super(TIMED_OUT, message);
    }

    public TTimeoutException(String message, boolean beforeWriting) {
        super(TIMED_OUT, message, beforeWriting);
    }

    public TTimeoutException(Throwable cause) {
        super(TIMED_OUT, cause);
    }

    public TTimeoutException(String message, Throwable cause) {
        super(TIMED_OUT, message, cause);
    }

    @Override
    public final int getType() {
        return TIMED_OUT;
    }

    public boolean isBeforeReading() {
        return beforeReading;
    }
}
