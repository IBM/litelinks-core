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
import java.util.Objects;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

import io.netty.util.internal.EmptyArrays;

/**
 * Litelinks-specific extension for X509TrustManger. It wraps the TrustManager
 * which delegates all methods but will return an empty list from
 * getAcceptedIssuers() if any of the configured trust certs are non-CA. When an
 * empty CertRequest is received the client will attempt to use any cert for
 * authentication. This is still secure since the usual verification is done
 * (and passes) on the server-side.
 *
 */
public class LitelinksTrustManager extends X509ExtendedTrustManager {
    private final X509ExtendedTrustManager delegate;
    private boolean sendCertRequest;

    public LitelinksTrustManager(X509TrustManager delegate) {
        this.delegate = (X509ExtendedTrustManager) delegate;
        if (delegate.getAcceptedIssuers() != null) {
            for (X509Certificate c : delegate.getAcceptedIssuers()) {
                int basicConstraints = c.getBasicConstraints();
                if (!Objects.equals(c.getIssuerX500Principal(), c.getSubjectX500Principal())
                        && basicConstraints == -1) {
                    sendCertRequest = false;
                    break;
                }
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
        delegate.checkClientTrusted(chain, authType, socket);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
        delegate.checkServerTrusted(chain, authType, socket);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
        delegate.checkClientTrusted(chain, authType, engine);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
        delegate.checkServerTrusted(chain, authType, engine);
    }
}