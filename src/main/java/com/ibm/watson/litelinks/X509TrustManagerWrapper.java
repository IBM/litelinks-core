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

import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import io.netty.handler.ssl.util.SimpleTrustManagerFactory;

public class X509TrustManagerWrapper extends SimpleTrustManagerFactory {
    public X509Certificate[] x509Certs = {};
    private X509TrustManager delegateTm;

    public X509TrustManagerWrapper(X509TrustManager delegateTm) {
        this.delegateTm = delegateTm;
    }

    @Override
    protected void engineInit(KeyStore keyStore) {
    }

    @Override
    protected void engineInit(ManagerFactoryParameters managerFactoryParameters) {
    }

    @Override
    protected TrustManager[] engineGetTrustManagers() {
        return new TrustManager[] { new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
                try {
                    delegateTm.checkClientTrusted(x509Certificates, s);
                } catch (Exception e) {
                    System.out.println("Exception occurred in checkClientTrusted(): "+e.getMessage());
                    X509Certificate c = x509Certificates[0];
                    String issuerDN = c.getIssuerDN().getName();
                    String subjectDN = c.getSubjectDN().getName();
                    int basicConstraints = c.getBasicConstraints();

                    if (!issuerDN.equals(subjectDN) && basicConstraints == -1) // if it's non-ca, accept it
                    {
                        System.out.println("Issuer DN is not equal to subject DN");
                        x509Certs = new X509Certificate[] { x509Certificates[0] };
                    }
                }
            }

            @Override
            public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return x509Certs;
            }
        } };
    }
}
