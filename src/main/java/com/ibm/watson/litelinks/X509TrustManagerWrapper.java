package com.ibm.watson.litelinks;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static io.netty.util.internal.ObjectUtil.*;

class X509TrustManagerWrapper extends X509ExtendedTrustManager {

    private X509Certificate[] x509Certs = {};
    private X509TrustManager delegate;

    X509TrustManagerWrapper(X509TrustManager delegate) {
        this.x509Certs = delegate.getAcceptedIssuers();
        this.delegate = checkNotNull(delegate, "delegate");
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String s) throws CertificateException {
        try {
            delegate.checkClientTrusted(chain, s);
        } catch (CertificateException ce) {
            X509Certificate c = chain[0];
            String issuerDN = c.getIssuerDN().getName();
            String subjectDN = c.getSubjectDN().getName();
            int basicConstraints = c.getBasicConstraints();

            if (!issuerDN.equals(subjectDN) && basicConstraints == -1) // if it's non-ca, accept it
            {
                X509Certificate[] certArray = new X509Certificate[x509Certs.length+1];
                System.arraycopy(x509Certs, 0, certArray, 0, x509Certs.length);
                certArray[x509Certs.length] = c;
                this.x509Certs = new X509Certificate[] { chain[0] };
            } else {
                throw ce;
            }
        }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String s, Socket socket) throws CertificateException {
        delegate.checkClientTrusted(chain, s);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String s, SSLEngine sslEngine)
            throws CertificateException {
        delegate.checkClientTrusted(chain, s);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String s) throws CertificateException {
        delegate.checkServerTrusted(chain, s);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String s, Socket socket)
            throws CertificateException {
        delegate.checkServerTrusted(chain, s);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String s, SSLEngine sslEngine)
            throws CertificateException {
        delegate.checkServerTrusted(chain, s);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return delegate.getAcceptedIssuers();
    }
}