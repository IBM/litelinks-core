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

package com.ibm.watson.litelinks.test;

import org.apache.thrift.TProcessor;

import java.io.File;

/**
 * Just for unit testing
 */
public class ShutdownTestThriftServiceImpl extends TestThriftServiceImpl {

    @Override
    protected TProcessor initialize() throws Exception {
        // start a non-daemon thread to ensure it doesn't
        // keep process from ending upon shutdown
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(5 * 60 * 1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
        return super.initialize();
    }

    @Override
    protected void shutdown() {
        System.out.println("inside service impl shutdown");
        try {
            // sleep long to test shutdown timeout
            Thread.sleep(60 * 1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void preShutdown() {
        String filename = System.getProperty("tmpfile");
        if (filename != null) {
            File tmp = new File(filename);
            if (tmp.exists()) {
                tmp.delete();
            }
        }
        try {
            Thread.sleep(2 * 1000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
