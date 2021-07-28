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

/**
 * If {@link #getCause()} is null, this indicates the connection was closed normally
 * rather than due to some other IO error.
 */
public class TConnectionClosedException extends WTTransportException {

    private static final long serialVersionUID = -1335952004110820429L;

    public TConnectionClosedException() {
        super(NOT_OPEN);
    }

    public TConnectionClosedException(String message) {
        super(NOT_OPEN, message);
    }

    public TConnectionClosedException(Throwable cause) {
        super(NOT_OPEN, cause);
    }

    public TConnectionClosedException(Throwable cause, boolean beforeWriting) {
        super(NOT_OPEN, cause, beforeWriting);
    }

    public TConnectionClosedException(String message, Throwable cause) {
        super(NOT_OPEN, message, cause);
    }

}
