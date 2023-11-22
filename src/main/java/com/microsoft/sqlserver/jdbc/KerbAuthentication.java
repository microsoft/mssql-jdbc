/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.logging.Level;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;


/**
 * KerbAuthentication for integrated authentication.
 */
final class KerbAuthentication extends SSPIAuthentication {
    private static final java.util.logging.Logger authLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.KerbAuthentication");

    private final SQLServerConnection con;
    private final String spn;

    private final GSSManager manager = GSSManager.getInstance();
    private LoginContext lc = null;
    private boolean isUserCreatedCredential = false;
    private GSSCredential peerCredentials = null;
    private boolean useDefaultNativeGSSCredential = false;
    private GSSContext peerContext = null;

    static {
        // Overrides the default JAAS configuration loader.
        // This one will forward to the default one in all cases but the default configuration is empty.
        Configuration.setConfiguration(new JaasConfiguration(Configuration.getConfiguration()));
    }

    /**
     * Initializes the Kerberos client security context
     * 
     * @throws SQLServerException
     */
    @SuppressWarnings("deprecation")
    private void initAuthInit() throws SQLServerException {
        try {
            // If we need to support NTLM as well, we can use null
            // Kerberos OID
            Oid kerberos = new Oid("1.2.840.113554.1.2.2");
            // http://blogs.sun.com/harcey/entry/of_java_kerberos_and_access
            // We pass null to indicate that the system should interpret the SPN
            // as it is.
            GSSName remotePeerName = manager.createName(spn, null);

            if (useDefaultNativeGSSCredential) {
                peerCredentials = manager.createCredential(null, GSSCredential.DEFAULT_LIFETIME, kerberos,
                        GSSCredential.INITIATE_ONLY);
            }

            if (null != peerCredentials) {
                peerContext = manager.createContext(remotePeerName, kerberos, peerCredentials,
                        GSSContext.DEFAULT_LIFETIME);
                peerContext.requestCredDeleg(false);
                peerContext.requestMutualAuth(true);
                peerContext.requestInteg(true);
            } else {
                String configName = con.activeConnectionProperties.getProperty(
                        SQLServerDriverStringProperty.JAAS_CONFIG_NAME.toString(),
                        SQLServerDriverStringProperty.JAAS_CONFIG_NAME.getDefaultValue());
                boolean useDefaultJaas = Boolean.parseBoolean(con.activeConnectionProperties.getProperty(
                        SQLServerDriverBooleanProperty.USE_DEFAULT_JAAS_CONFIG.toString(),
                        Boolean.toString(SQLServerDriverBooleanProperty.USE_DEFAULT_JAAS_CONFIG.getDefaultValue())));

                if (!configName.equals(SQLServerDriverStringProperty.JAAS_CONFIG_NAME.getDefaultValue())
                        && useDefaultJaas) {
                    // Reset configName to default -- useDefaultJaas setting takes priority over jaasConfigName
                    if (authLogger.isLoggable(Level.WARNING)) {
                        authLogger.warning(toString()
                                + String.format("Using default JAAS configuration, configured %s=%s will not be used.",
                                        SQLServerDriverStringProperty.JAAS_CONFIG_NAME, configName));
                    }
                    configName = SQLServerDriverStringProperty.JAAS_CONFIG_NAME.getDefaultValue();
                }
                Subject currentSubject;
                KerbCallback callback = new KerbCallback(con);
                try {
                    AccessControlContext context = AccessController.getContext();
                    currentSubject = Subject.getSubject(context);
                    if (null == currentSubject) {
                        if (useDefaultJaas) {
                            lc = new LoginContext(configName, null, callback, new JaasConfiguration(null));
                        } else {
                            lc = new LoginContext(configName, callback);
                        }
                        lc.login();
                        // per documentation LoginContext will instantiate a new subject.
                        currentSubject = lc.getSubject();
                    }
                } catch (LoginException le) {
                    if (authLogger.isLoggable(Level.FINE)) {
                        authLogger.fine(toString() + "Failed to login using Kerberos due to " + le.getClass().getName()
                                + ":" + le.getMessage());
                    }
                    try {
                        // Not very clean since it raises an Exception, but we are sure we are cleaning well everything
                        con.terminate(SQLServerException.DRIVER_ERROR_NONE,
                                SQLServerException.getErrString("R_integratedAuthenticationFailed"), le);
                    } catch (SQLServerException alwaysTriggered) {
                        String message = MessageFormat.format(SQLServerException.getErrString("R_kerberosLoginFailed"),
                                alwaysTriggered.getMessage(), le.getClass().getName(), le.getMessage());
                        if (callback.getUsernameRequested() != null) {
                            message = MessageFormat.format(
                                    SQLServerException.getErrString("R_kerberosLoginFailedForUsername"),
                                    callback.getUsernameRequested(), message);
                        }
                        // By throwing Exception with LOGON_FAILED -> we avoid looping for connection
                        // In this case, authentication will never work anyway -> fail fast
                        throw new SQLServerException(message, alwaysTriggered.getSQLState(),
                                SQLServerException.LOGON_FAILED, le);
                    }
                    return;
                }

                if (authLogger.isLoggable(Level.FINER)) {
                    authLogger.finer(toString() + " Getting client credentials");
                }
                peerCredentials = getClientCredential(currentSubject, manager, kerberos);

                if (authLogger.isLoggable(Level.FINER)) {
                    authLogger.finer(toString() + " creating security context");
                }
                peerContext = manager.createContext(remotePeerName, kerberos, peerCredentials,
                        GSSContext.DEFAULT_LIFETIME);

                // The following flags should be inline with our native implementation.
                peerContext.requestCredDeleg(true);
                peerContext.requestMutualAuth(true);
                peerContext.requestInteg(true);
            }
        } catch (GSSException ge) {
            if (authLogger.isLoggable(Level.FINER)) {
                authLogger.finer(toString() + "initAuthInit failed GSSException:-" + ge);
            }
            con.terminate(SQLServerException.DRIVER_ERROR_NONE,
                    SQLServerException.getErrString("R_integratedAuthenticationFailed"), ge);
        } catch (PrivilegedActionException ge) {
            if (authLogger.isLoggable(Level.FINER)) {
                authLogger.finer(toString() + "initAuthInit failed privileged exception:-" + ge);
            }
            con.terminate(SQLServerException.DRIVER_ERROR_NONE,
                    SQLServerException.getErrString("R_integratedAuthenticationFailed"), ge);
        }
    }

    // We have to do a privileged action to create the credential of the user in the current context
    private static GSSCredential getClientCredential(final Subject subject, final GSSManager gssManager,
            final Oid kerboid) throws PrivilegedActionException {
        final PrivilegedExceptionAction<GSSCredential> action = new PrivilegedExceptionAction<GSSCredential>() {
            public GSSCredential run() throws GSSException {
                return gssManager.createCredential(null, // use the default principal
                        GSSCredential.DEFAULT_LIFETIME, kerboid, GSSCredential.INITIATE_ONLY);
            }
        };
        // TO support java 5, 6 we have to do this
        // The signature for Java 5 returns an object 6 returns GSSCredential, immediate casting throws
        // warning in Java 6.
        Object credential = Subject.doAs(subject, action);
        return (GSSCredential) credential;
    }

    private byte[] initAuthHandShake(byte[] pin, boolean[] done) throws SQLServerException {
        try {
            if (authLogger.isLoggable(Level.FINER)) {
                authLogger.finer(toString() + " Sending token to server over secure context");
            }
            byte[] byteToken = peerContext.initSecContext(pin, 0, pin.length);

            if (peerContext.isEstablished()) {
                done[0] = true;
                if (authLogger.isLoggable(Level.FINER)) {
                    authLogger.finer(toString() + "Authentication done.");
                }
            } else if (null == byteToken) {
                // The documentation is not clear on when this can happen but it does say this could happen
                if (authLogger.isLoggable(Level.INFO)) {
                    authLogger.info(toString() + "byteToken is null in initSecContext.");
                }
                con.terminate(SQLServerException.DRIVER_ERROR_NONE,
                        SQLServerException.getErrString("R_integratedAuthenticationFailed"));
            }
            return byteToken;
        } catch (GSSException ge) {
            if (authLogger.isLoggable(Level.FINER)) {
                authLogger.finer(toString() + "initSecContext Failed :-" + ge);
            }
            con.terminate(SQLServerException.DRIVER_ERROR_NONE,
                    SQLServerException.getErrString("R_integratedAuthenticationFailed"), ge);
        }
        // keep the compiler happy
        return null;
    }

    // Package visible members below.
    KerbAuthentication(SQLServerConnection con, String address, int port) {
        this.con = con;
        this.spn = null != con ? getSpn(con) : null;
    }

    /**
     * 
     * @param con
     * @param address
     * @param port
     * @param impersonatedUserCred
     */
    KerbAuthentication(SQLServerConnection con, String address, int port, GSSCredential impersonatedUserCred,
            boolean isUserCreated, boolean useDefaultNativeGSSCredential) {
        this(con, address, port);
        this.peerCredentials = impersonatedUserCred;
        this.isUserCreatedCredential = isUserCreated;
        this.useDefaultNativeGSSCredential = useDefaultNativeGSSCredential;
    }

    byte[] generateClientContext(byte[] pin, boolean[] done) throws SQLServerException {
        if (null == peerContext) {
            initAuthInit();
        }
        return initAuthHandShake(pin, done);
    }

    void releaseClientContext() {
        try {
            if (null != peerCredentials && !isUserCreatedCredential && !useDefaultNativeGSSCredential) {
                peerCredentials.dispose();
            } else if (null != peerCredentials && (isUserCreatedCredential || useDefaultNativeGSSCredential)) {
                peerCredentials = null;
            }
            if (null != peerContext) {
                peerContext.dispose();
            }
            if (null != lc) {
                lc.logout();
            }
        } catch (LoginException e) {
            // yes we are eating exceptions here but this should not fail in the normal circumstances and we do not want
            // to eat previous login errors if caused before which is more useful to the user than the cleanup errors.
            if (authLogger.isLoggable(Level.FINE)) {
                authLogger.fine(toString() + " Release of the credentials failed LoginException: " + e);
            }
        } catch (GSSException e) {
            // yes we are eating exceptions here but this should not fail in the normal circumstances and we do not want
            // to eat previous login errors if caused before which is more useful to the user than the cleanup errors.
            if (authLogger.isLoggable(Level.FINE)) {
                authLogger.fine(toString() + " Release of the credentials failed GSSException: " + e);
            }
        }
    }
}
