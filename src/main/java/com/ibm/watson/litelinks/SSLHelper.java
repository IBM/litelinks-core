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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.TrustManagerFactoryWrapper;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Helper for retrieving SSL config parameters and building an {@link SSLContext}.
 */
public class SSLHelper {

    private static final Logger logger = LoggerFactory.getLogger(SSLHelper.class);

    private SSLHelper() {}

    // just for testing, should never be set to true!
    private static final boolean TRUST_EVERYTHING = false;

    private static final Boolean USE_OPEN_SSL;

    static {
        String useJdkVal = System.getProperty(LitelinksSystemPropNames.USE_JDK_TLS);
        USE_OPEN_SSL = "true".equalsIgnoreCase(useJdkVal) || !OpenSsl.supportsKeyManagerFactory()
                ? Boolean.FALSE : useJdkVal != null? Boolean.TRUE : null;
        logger.info("Litelinks using OpenSSL for TLS: "
                + (USE_OPEN_SSL != null? USE_OPEN_SSL : "when possible"));
    }

    static final String[] TLS_PROTOS = { "TLSv1", "TLSv1.1", "TLSv1.2" };

    public static class SSLParams {

        public static final String DEFAULT_PROTOCOL = "TLSv1.2"; // was previously "TLS"
        public static final String DEFAULT_STORE_TYPE = "JKS";

        public static final String[] SSL_PROPS_FILE_JVM_ARGS
                = { "com.ibm.watson.litelinks.ssl.configfile", "com.ibm.watson.ssl.configfile" };

        public static final String[] PREFIX = { "litelinks.ssl.", "watson.ssl." };

        public static final String PARAM_PROTOCOL = "protocol";
        public static final String PARAM_CLIENT_AUTH = "clientauth";
        public static final String PARAM_CIPHERS = "ciphersuites";
        public static final String PARAM_KEYMGRALG = "keymanager.algorithm";
        public static final String PARAM_KEYPATH = "keystore.path";
        public static final String PARAM_KEYPASS = "keystore.password";
        public static final String PARAM_KEYTYPE = "keystore.type";
        public static final String PARAM_TRUSTMGRALG = "trustmanager.algorithm";
        public static final String PARAM_TRUSTPATH = "truststore.path";
        public static final String PARAM_TRUSTPASS = "truststore.password";
        public static final String PARAM_TRUSTTYPE = "truststore.type";

        public static final String PARAM_PKEY_PATH = "key.path";
        public static final String PARAM_PKEY_CERTS = "key.certpath";
        public static final String PARAM_PKEY_PASS = "key.password";
        public static final String PARAM_CERTS_PATH = "trustcerts.path";

        public static final List<String> PARAM_LIST = Arrays.asList(
                PARAM_PROTOCOL, PARAM_CLIENT_AUTH, PARAM_CIPHERS,
                PARAM_KEYMGRALG, PARAM_KEYPATH, PARAM_KEYPASS, PARAM_KEYTYPE,
                PARAM_TRUSTMGRALG, PARAM_TRUSTPATH, PARAM_TRUSTPASS, PARAM_TRUSTTYPE,
                PARAM_PKEY_PATH, PARAM_PKEY_CERTS, PARAM_PKEY_PASS, PARAM_CERTS_PATH);

        public String protocol;
        public KeyStoreInfo keyStore, trustStore;
        public boolean clientAuth;
        public String keyManagerAlg, trustManagerAlg;

        public File keyFile, keyCertsFile;
        public String keyPassword;
        public File trustCertsFile;

        public String[] cipherSuites;

        private static volatile SSLParams defaultParams;

        /**
         * Parameters are loaded from a properties file if specified, or from
         * individually specified env vars or JVM args. The precedence per parameter
         * is env var followed by JVM arg followed by props file.
         * <p>
         * Env var names are the same as the corresponding JVM arg name but upper-cased
         * and with .'s replaced with _'s.
         *
         * @throws IOException
         */
        public static SSLParams getDefault() throws IOException {
            if (defaultParams == null) {
                synchronized (SSLParams.class) {
                    if (defaultParams != null) {
                        return defaultParams;
                    }
                    String configFile = System.getProperty(SSL_PROPS_FILE_JVM_ARGS[0]);
                    if (configFile == null) {
                        configFile = System.getProperty(SSL_PROPS_FILE_JVM_ARGS[1]);
                    }
                    Properties props = new Properties();
                    if (configFile != null) {
                        Properties fileProps = new Properties();
                        try (FileInputStream fis = new FileInputStream(configFile)) {
                            fileProps.load(fis);
                        }
                        // validate parameters in properties file
                        for (Entry<Object, Object> ent : fileProps.entrySet()) {
                            String k = (String) ent.getKey();
                            if (k.startsWith(PREFIX[0])) {
                                k = k.substring(PREFIX[0].length());
                            } else if (k.startsWith(PREFIX[1])) {
                                k = k.substring(PREFIX[1].length());
                            } else {
                                k = null;
                            }
                            if (!PARAM_LIST.contains(k)) {
                                throw new IOException(
                                        "SSL properties file contains invalid parameter: " + ent.getKey());
                            }
                            props.put(k, ent.getValue());
                        }
                    }
                    // overlay any specified as env vars or JVM args
                    for (String arg : PARAM_LIST) {
                        String val = System.getenv((PREFIX[0] + arg).toUpperCase().replace('.', '_'));
                        if (val == null) {
                            val = System.getProperty(PREFIX[0] + arg);
                        }
                        if (val == null) {
                            val = System.getProperty(PREFIX[1] + arg);
                        }
                        if (val != null) {
                            props.put(arg, val);
                        }
                    }

                    defaultParams = load(props);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Loaded SSL parameters:");
                        for (Entry<Object, Object> ent : props.entrySet()) {
                            logger.debug("  " + ent.getKey() + "=" + ent.getValue());
                        }
                    }
                }
            }
            return defaultParams;
        }

        public static SSLParams load(Properties props) {
            SSLParams slp = new SSLParams();
            slp.protocol = props.getProperty(PARAM_PROTOCOL, DEFAULT_PROTOCOL);
            slp.clientAuth = "true".equals(props.getProperty(PARAM_CLIENT_AUTH));
            slp.keyManagerAlg = props.getProperty(PARAM_KEYMGRALG,
                    KeyManagerFactory.getDefaultAlgorithm());
            slp.trustManagerAlg = props.getProperty(PARAM_TRUSTMGRALG,
                    TrustManagerFactory.getDefaultAlgorithm());
            String cs = props.getProperty(PARAM_CIPHERS);
            slp.cipherSuites = cs != null? cs.split(",") : null;

            String path = props.getProperty(PARAM_TRUSTPATH);
            if (path != null) {
                String pass = props.getProperty(PARAM_TRUSTPASS);
                String type = props.getProperty(PARAM_TRUSTTYPE, DEFAULT_STORE_TYPE);
                slp.trustStore = new KeyStoreInfo(new File(path),
                        pass != null? pass.toCharArray() : null, type);
            }
            path = props.getProperty(PARAM_KEYPATH);
            if (path != null) {
                String pass = props.getProperty(PARAM_KEYPASS);
                String type = props.getProperty(PARAM_KEYTYPE, DEFAULT_STORE_TYPE);
                slp.keyStore = new KeyStoreInfo(new File(path),
                        pass != null? pass.toCharArray() : null, type);
            }
            path = props.getProperty(PARAM_PKEY_PATH);
            if (path != null) {
                slp.keyFile = new File(path);
                String keyCertPath = props.getProperty(PARAM_PKEY_CERTS);
                if (keyCertPath != null) {
                    slp.keyCertsFile = new File(keyCertPath);
                }
                slp.keyPassword = props.getProperty(PARAM_PKEY_PASS);
            }
            path = props.getProperty(PARAM_CERTS_PATH);
            if (path != null) {
                slp.trustCertsFile = new File(path);
            }
            return slp;
        }

        @VisibleForTesting
        public static void resetDefaultParameters() {
            defaultParams = null;
        }
    }

    private static final LoadingCache<List<Object>, SslContext> sslcCache
            = CacheBuilder.newBuilder().weakValues()
            .removalListener(new RemovalListener<List<Object>, SslContext>() {
                @Override
                public void onRemoval(RemovalNotification<List<Object>, SslContext> notif) {
                    ReferenceCountUtil.release(notif.getValue());
                }
            }).build(new CacheLoader<List<Object>, SslContext>() {
                @Override
                public SslContext load(List<Object> key) throws Exception {
                    return buildSslContext(key);
                }
            });

    /**
     * <b>Important:</b> {@link SSLEngine}s produced by the returned context must be explicitly released
     * if they implement {@link ReferenceCounted}. A containing {@link SslHandler} takes ownership of
     * this once it's added to a channel pipeline.
     */
    public static SslContext getSslContext(String protocol, KeyStoreInfo keyStore,
            KeyStoreInfo trustStore, String keyMgrAlg, String trustMgrAlg, boolean server,
            boolean reqClientAuth) throws IOException, GeneralSecurityException {
        return getSslContext(protocol, keyStore, trustStore,
                keyMgrAlg, trustMgrAlg, server, server && reqClientAuth,
                null, null, null, null);
    }

    /**
     * <b>Important:</b> {@link SSLEngine}s produced by the returned context must be explicitly released
     * if they implement {@link ReferenceCounted}. A containing {@link SslHandler} takes ownership of
     * this once it's added to a channel pipeline.
     */
    public static SslContext getSslContext(String protocolOverride, boolean server, boolean reqClientAuth)
            throws IOException, GeneralSecurityException {
        SSLParams params = SSLParams.getDefault();
        String protocol = protocolOverride != null? protocolOverride : params.protocol;
        reqClientAuth = server && (reqClientAuth || params.clientAuth);
        return getSslContext(protocol, params.keyStore, params.trustStore,
                params.keyManagerAlg, params.trustManagerAlg, server, reqClientAuth,
                params.keyFile, params.keyCertsFile, params.keyPassword, params.trustCertsFile);
    }

    private static SslContext getSslContext(Object... args) throws IOException, GeneralSecurityException {
        try {
            return sslcCache.get(Arrays.asList(args));
        } catch (ExecutionException e) {
            Exception cause = (Exception) e.getCause();
            Throwables.throwIfInstanceOf(cause, IOException.class);
            Throwables.throwIfInstanceOf(cause, GeneralSecurityException.class);
            Throwables.throwIfUnchecked(cause);
            throw new RuntimeException(cause);
        }
    }

    private static SslContext buildSslContext(List<Object> params) throws IOException, GeneralSecurityException {
        String protocol = (String) params.get(0);
        String keyMgrAlg = (String) params.get(3), trustMgrAlg = (String) params.get(4);
        KeyStoreInfo keyStoreInfo = (KeyStoreInfo) params.get(1), trustStoreInfo = (KeyStoreInfo) params.get(2);
        Boolean server = (Boolean) params.get(5), reqClientAuth = (Boolean) params.get(6);
        File keyFile = (File) params.get(7), keyCertFile = (File) params.get(8);
        String keyPassword = (String) params.get(9);
        File trustCertsFile = (File) params.get(10);

        if (keyFile != null) {
            if (keyStoreInfo != null) {
                throw new IOException("Cannot provide both SSL keystore and key file");
            }
            if (keyCertFile == null) {
                throw new IOException("SSL key cert file must be provided with key file");
            }
        }
        Boolean useOpenSsl = USE_OPEN_SSL;
        KeyManagerFactory kmf = null;
        if (keyStoreInfo != null) {
            KeyStore keyStore = loadKeyStore(keyStoreInfo);
            kmf = getKeyManagerFactory(keyStore, keyStoreInfo.getPassword(), keyMgrAlg);
            if (useOpenSsl == null && containsDsaCert(keyStore)) {
                logger.info("Disabling litelinks " + (server? "server" : "client")
                        + " use of OpenSSL for TLS due to keystore containing DSA cert: "
                        + keyStoreInfo.getFile());
                useOpenSsl = Boolean.FALSE;
            }
        }

        SslContextBuilder scb;
        if (server) {
            scb = kmf != null? SslContextBuilder.forServer(kmf)
                    : SslContextBuilder.forServer(keyCertFile, keyFile, keyPassword);
            if (reqClientAuth) {
                scb.clientAuth(ClientAuth.REQUIRE);
            }
        } else {
            scb = SslContextBuilder.forClient();
            if (kmf != null) {
                scb.keyManager(kmf);
            } else if (keyFile != null) {
                scb.keyManager(keyCertFile, keyFile, keyPassword);
            }
        }
        KeyStore trustStore = null;
        if (trustStoreInfo != null) {
            trustStore = loadKeyStore(trustStoreInfo);
            if (useOpenSsl == null && containsDsaCert(trustStore)) {
                logger.info("Disabling litelinks " + (server? "server" : "client")
                        + " use of OpenSSL for TLS due to truststore containing DSA cert: "
                        + trustStoreInfo.getFile());
                useOpenSsl = Boolean.FALSE;
            }
        }
        configureTrustManager(scb, trustStore, trustMgrAlg, trustCertsFile);
        // Use OpenSSL provider unless specifically disabled for some reason
        // (unsupported on platform, disabled via env var, or DSA cert found in keystore)
        boolean openSsl = useOpenSsl == null || useOpenSsl;
        scb.ciphers(getDefaultCiphers(server, openSsl));
        if (protocol != null) {
            scb.protocols("TLS".equals(protocol)? TLS_PROTOS : new String[] { protocol });
        }
        return scb.sslProvider(openSsl? SslProvider.OPENSSL : SslProvider.JDK).build();
    }

    private static List<String> getDefaultCiphers(boolean server, boolean openSsl) {
        String[] ciphers = !server? ((SSLSocketFactory) SSLSocketFactory.getDefault()).getDefaultCipherSuites()
                : ((SSLServerSocketFactory) SSLServerSocketFactory.getDefault()).getDefaultCipherSuites();
        return !openSsl? Arrays.asList(ciphers)
                : Arrays.stream(ciphers).filter(OpenSsl::isCipherSuiteAvailable).collect(Collectors.toList());
    }

    private static KeyManagerFactory getKeyManagerFactory(KeyStore keyStore, char[] password, String keyMgrAlg)
            throws IOException, GeneralSecurityException {
        if (keyStore == null) {
            return null;
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyMgrAlg);
        kmf.init(keyStore, password);
        return kmf;
    }

    private static SslContextBuilder configureTrustManager(SslContextBuilder scb, KeyStore trustStore,
            String trustMgrAlg, File trustCertsFile) throws IOException, GeneralSecurityException {
        if (TRUST_EVERYTHING) {
            return scb.trustManager(InsecureTrustManagerFactory.INSTANCE);
        }
        // trust certs file or dir but no truststore
        if (trustCertsFile != null && trustStore == null) {
            return !trustCertsFile.isDirectory()? scb.trustManager(trustCertsFile)
                    : scb.trustManager(generateCertificates(trustCertsFile).toArray(new X509Certificate[0]));
        }
        // all other cases
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(trustMgrAlg);
        if (trustStore != null) {
            // truststore *and* certs file or dir provided, add certs to truststore
            if (trustCertsFile != null) {
                final String prefix = trustCertsFile.getName();
                int index = 0;
                for (X509Certificate cert : generateCertificates(trustCertsFile)) {
                    String alias;
                    do {
                        alias = prefix + '_' + index++;
                    } while (trustStore.isCertificateEntry(alias));
                    trustStore.setCertificateEntry(alias, cert);
                }
            }
        }
        tmf.init(trustStore); // passing null here will init with java defaults
      
        //get the default trustManager
        X509TrustManager delegateTm = null;
        for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
                delegateTm = (X509TrustManager) tm;
                break;
            }
        }
        X509TrustManagerWrapper trustManagerWrapper = new X509TrustManagerWrapper(delegateTm);
        TrustManagerFactoryWrapper tmfWrapper = new TrustManagerFactoryWrapper(trustManagerWrapper);
        
        return scb.trustManager(tmfWrapper);
    }

    private static final FilenameFilter CERT_FILES = (d, n)
            -> n.toLowerCase().endsWith(".pem") || n.toLowerCase().endsWith(".crt");

    // Load all certificates from file or directory,
    // in dir case will only include files with .pem or .crt extension
    @SuppressWarnings("unchecked")
    private static Collection<X509Certificate> generateCertificates(File fileOrDir)
            throws IOException, GeneralSecurityException {
        if (!fileOrDir.isDirectory()) {
            try (InputStream certStream = new FileInputStream(fileOrDir)) {
                return (Collection<X509Certificate>) CertificateFactory.getInstance("X.509")
                        .generateCertificates(certStream);
            }
        }
        List<X509Certificate> certs = new ArrayList<>();
        for (File file : fileOrDir.listFiles(CERT_FILES)) {
            certs.addAll(generateCertificates(file));
        }
        return certs;
    }

    private static KeyStore loadKeyStore(KeyStoreInfo ksi) throws IOException, GeneralSecurityException {
        final KeyStore ks = KeyStore.getInstance(ksi.getType());
        try (FileInputStream fis = new FileInputStream(ksi.getFile())) {
            ks.load(fis, ksi.getPassword());
        }
        return ks;
    }

    private static boolean containsDsaCert(KeyStore keyStore) throws KeyStoreException {
        for (Enumeration<String> en = keyStore.aliases(); en.hasMoreElements(); ) {
            if ("DSA".equals(keyStore.getCertificate(en.nextElement()).getPublicKey().getAlgorithm())) {
                return true;
            }
        }
        return false;
    }

    public static class KeyStoreInfo {

        private final File file;
        private final char[] password;
        private final String type;

        public KeyStoreInfo(File file, char[] password, String type) {
            this.file = file;
            this.password = password;
            this.type = type;
        }
        public File getFile() {
            return file;
        }
        public char[] getPassword() {
            return password;
        }
        public String getType() {
            return type;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = prime + (file == null? 0 : file.hashCode());
            result = prime * result + Arrays.hashCode(password);
            return prime * result + (type == null? 0 : type.hashCode());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            KeyStoreInfo other = (KeyStoreInfo) obj;
            if(!Objects.equals(file, other.file)) return false;
            if (!Arrays.equals(password, other.password)) return false;
            if(!Objects.equals(type, other.type)) return false;
            return true;
        }
    }

}
