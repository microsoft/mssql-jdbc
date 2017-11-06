package com.microsoft.sqlserver.jdbc.ssl.trustmanager;

import java.io.IOException;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.GeneralSecurityException;
import javax.net.ssl.X509TrustManager;

/**
 * This class implements an X509TrustManager that always accepts the X509Certificate chain offered to it.
 * 
 * The constructor argument certToTrust is a dummy string used to test trustManagerConstructorArg.
 * 
 */

public class TrustManagerWithConstructorArg implements X509TrustManager {
    X509Certificate cert;
    X509TrustManager trustManager;

    public TrustManagerWithConstructorArg(String certToTrust) throws IOException, GeneralSecurityException {
        trustManager = new X509TrustManager() {
            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain,
                    String authType) throws CertificateException {
            }

            @Override
            public void checkClientTrusted(X509Certificate[] chain,
                    String authType) throws CertificateException {
            }
        };
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain,
            String authType) throws CertificateException {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain,
            String authType) throws CertificateException {
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return null;
    }
}
