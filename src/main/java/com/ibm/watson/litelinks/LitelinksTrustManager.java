/*
 * *****************************************************************
 *
 * IBM Confidential
 * OCO Source Materials
 *
 * Licensed Materials - Property of IBM
 *
 * litelinks-core
 * (C) Copyright IBM Corp. 2001, 2018 All Rights Reserved.
 *
 * The source code for this program is not published or otherwise
 * divested of its trade secrets, irrespective of what has been
 * deposited with the U.S. Copyright Office.
 *
 * US Government Users Restricted Rights - Use, duplication or
 * disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
 *
 * ***************************************************************** */
package com.ibm.watson.litelinks;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

public class LitelinksTrustManager extends X509ExtendedTrustManager {
    public X509Certificate[] x509Certs = {};
    public X509TrustManager delegateTm;

    public LitelinksTrustManager(X509TrustManager delegateTm) {
        this.delegateTm = delegateTm;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegateTm.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegateTm.checkServerTrusted(chain, authType);

    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return x509Certs;
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