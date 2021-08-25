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

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

import io.netty.util.internal.EmptyArrays;

/**
 * Litelinks-specific extension for X509TrustManger
 *
 */
public class LitelinksTrustManager extends X509ExtendedTrustManager {
    private final X509ExtendedTrustManager delegate;
    private boolean sendCertRequest = true;

    public LitelinksTrustManager(X509TrustManager delegateTm) {
        this.delegate = (X509ExtendedTrustManager)delegateTm;
        for(X509Certificate c : delegateTm.getAcceptedIssuers())
        {
            String issuerDN = c.getIssuerX500Principal().getName();
            String subjectDN = c.getSubjectX500Principal().getName();
            int basicConstraints = c.getBasicConstraints();
            if(!issuerDN.equals(subjectDN) && basicConstraints == -1) //not all CA
            {
                sendCertRequest = false;
                break;
            }
        }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegate.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegate.checkServerTrusted(chain, authType);

    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return sendCertRequest ? delegate.getAcceptedIssuers() : EmptyArrays.EMPTY_X509_CERTIFICATES;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
    }
}