package com.microsoft.sqlserver.jdbc;

import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.X509TrustManager;


/**
 * This class implements an X509TrustManager that always accepts the X509Certificate chain offered to it.
 *
 * A PermissiveX509TrustManager is used to "verify" the authenticity of the server when the trustServerCertificate
 * connection property is set to true.
 */
final class PermissiveX509TrustManager implements X509TrustManager {
    private final Logger logger;
    private final String logContext;

    PermissiveX509TrustManager(TDSChannel tdsChannel) {
        this.logger = tdsChannel.getLogger();
        this.logContext = tdsChannel.toString() + " (PermissiveX509TrustManager):";
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        if (logger.isLoggable(Level.FINER))
            logger.finer(logContext + " Trusting client certificate (!)");
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        if (logger.isLoggable(Level.FINER))
            logger.finer(logContext + " Trusting server certificate");
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }
}


/**
 * This class implements an X509TrustManager that validates hostname.
 *
 * This validates the subject name in the certificate with the host name
 */
final class HostNameOverrideX509TrustManager implements X509TrustManager {
    private final Logger logger;
    private final String logContext;
    private final X509TrustManager defaultTrustManager;
    private String hostName;

    HostNameOverrideX509TrustManager(TDSChannel tdsChannel, X509TrustManager tm, String hostName) {
        this.logger = tdsChannel.getLogger();
        this.logContext = tdsChannel.toString() + " (HostNameOverrideX509TrustManager):";
        defaultTrustManager = tm;

        // canonical name is in lower case so convert this to lowercase too.
        this.hostName = hostName.toLowerCase(Locale.ENGLISH);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        if (logger.isLoggable(Level.FINEST)) {
            logger.finest(logContext + " Forwarding ClientTrusted.");
        }

        defaultTrustManager.checkClientTrusted(chain, authType);

        // Explicitly validate the expiry dates
        for (X509Certificate cert : chain) {
            cert.checkValidity();
        }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        if (logger.isLoggable(Level.FINEST)) {
            logger.finest(logContext + " Forwarding Trusting server certificate");
        }

        defaultTrustManager.checkServerTrusted(chain, authType);

        // Explicitly validate the expiry dates
        for (X509Certificate cert : chain) {
            cert.checkValidity();
        }

        if (logger.isLoggable(Level.FINEST)) {
            logger.finest(logContext + " Default serverTrusted succeeded proceeding with server name validation");
        }

        SQLServerCertificateUtils.validateServerNameInCertificate(chain[0], hostName);
    }

    public X509Certificate[] getAcceptedIssuers() {
        return defaultTrustManager.getAcceptedIssuers();
    }
}


/**
 * This class implements an X509TrustManager that validates the server certificate provided. This is applicable when
 * encrypt connection property is set to "strict"
 *
 */
final class ServerCertificateX509TrustManager implements X509TrustManager {
    private final Logger logger;
    private final String logContext;
    private String hostName;
    private String serverCert;

    ServerCertificateX509TrustManager(TDSChannel tdsChannel, String cert, String hostName) {
        this.logger = tdsChannel.getLogger();
        this.logContext = tdsChannel.toString() + " (ServerCertificateX509TrustManager):";
        // canonical name is in lower case so convert this to lowercase too.
        this.hostName = hostName.toLowerCase(Locale.ENGLISH);
        this.serverCert = cert;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        if (logger.isLoggable(Level.FINEST))
            logger.finest(logContext + " Trusting client certificate (!)");
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        if (logger.isLoggable(Level.FINEST)) {
            logger.finest(logContext + " Check if server trusted.");
        }

        if (null == chain || 0 == chain.length || null == authType || authType.isEmpty()) {
            throw new IllegalArgumentException(SQLServerException.getErrString("R_illegalArgumentTrustManager"));
        }
        X509Certificate cert = null;

        try {

            // validate expiry dates
            for (X509Certificate c : chain) {
                cert = c;
                c.checkValidity();
            }

            // 1st element in the certificate chain is the server cert
            // 2nd element in the certificate chain is the trusted root cert
            if (null == serverCert) {
                SQLServerCertificateUtils.validateServerNameInCertificate(chain[0], hostName);
            } else {
                SQLServerCertificateUtils.validateServerCerticate(chain[0], serverCert);
            }
        } catch (CertificateNotYetValidException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_serverCertNotYetValid"));
            Object[] msgArgs = {serverCert != null ? serverCert : hostName, e.getMessage()};
            throw new CertificateException(form.format(msgArgs));
        } catch (CertificateExpiredException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_serverCertExpired"));
            Object[] msgArgs = {serverCert != null ? serverCert : hostName, e.getMessage()};
            throw new CertificateException(form.format(msgArgs));
        } catch (Exception e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_serverCertError"));
            Object[] msgArgs = {e.getMessage(), serverCert != null ? serverCert : hostName,
                    cert != null ? cert.toString() : ""};
            throw new CertificateException(form.format(msgArgs));
        }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }
}
