/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.net.IDN;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.NamingException;
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

import com.microsoft.sqlserver.jdbc.dns.DNSKerberosLocator;


/**
 * KerbAuthentication for int auth.
 */
final class KerbAuthentication extends SSPIAuthentication {
    private final static java.util.logging.Logger authLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.KerbAuthentication");

    private final SQLServerConnection con;
    private final String spn;

    private final GSSManager manager = GSSManager.getInstance();
    private LoginContext lc = null;
    private boolean isUserCreatedCredential = false;
    private GSSCredential peerCredentials = null;
    private GSSContext peerContext = null;

    static {
        // Overrides the default JAAS configuration loader.
        // This one will forward to the default one in all cases but the default configuration is empty.
        Configuration.setConfiguration(new JaasConfiguration(Configuration.getConfiguration()));
    }

    private void intAuthInit() throws SQLServerException {
        try {
            // If we need to support NTLM as well, we can use null
            // Kerberos OID
            Oid kerberos = new Oid("1.2.840.113554.1.2.2");
            // http://blogs.sun.com/harcey/entry/of_java_kerberos_and_access
            // We pass null to indicate that the system should interpret the SPN
            // as it is.
            GSSName remotePeerName = manager.createName(spn, null);

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
                Subject currentSubject;
                KerbCallback callback = new KerbCallback(con);
                try {
                    AccessControlContext context = AccessController.getContext();
                    currentSubject = Subject.getSubject(context);
                    if (null == currentSubject) {
                        lc = new LoginContext(configName, callback);
                        lc.login();
                        // per documentation LoginContext will instantiate a new subject.
                        currentSubject = lc.getSubject();
                    }
                } catch (LoginException le) {
                    LogUtil.fine(authLogger, "{0}: Failed to login using Kerberos due to {1}", this, le);

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

                authLogger.log(Level.FINER, "{0}: Getting client credentials", this);
                peerCredentials = getClientCredential(currentSubject, manager, kerberos);
                authLogger.log(Level.FINER, "{0}: creating security context", this);

                peerContext = manager.createContext(remotePeerName, kerberos, peerCredentials,
                        GSSContext.DEFAULT_LIFETIME);
                // The following flags should be inline with our native implementation.
                peerContext.requestCredDeleg(true);
                peerContext.requestMutualAuth(true);
                peerContext.requestInteg(true);
            }
        }

        catch (GSSException ge) {
            LogUtil.finer(authLogger, "{0}: initAuthInit failed GSSException: {1}", this, ge);
            con.terminate(SQLServerException.DRIVER_ERROR_NONE,
                    SQLServerException.getErrString("R_integratedAuthenticationFailed"), ge);
        } catch (PrivilegedActionException ge) {
            LogUtil.finer(authLogger, "{0}: initAuthInit failed privileged exception: {1}", this, ge);
            con.terminate(SQLServerException.DRIVER_ERROR_NONE,
                    SQLServerException.getErrString("R_integratedAuthenticationFailed"), ge);
        }

    }

    // We have to do a privileged action to create the credential of the user in the current context
    private static GSSCredential getClientCredential(final Subject subject, final GSSManager MANAGER,
            final Oid kerboid) throws PrivilegedActionException {
        final PrivilegedExceptionAction<GSSCredential> action = new PrivilegedExceptionAction<GSSCredential>() {
            public GSSCredential run() throws GSSException {
                return MANAGER.createCredential(null // use the default principal
                , GSSCredential.DEFAULT_LIFETIME, kerboid, GSSCredential.INITIATE_ONLY);
            }
        };
        // TO support java 5, 6 we have to do this
        // The signature for Java 5 returns an object 6 returns GSSCredential, immediate casting throws
        // warning in Java 6.
        Object credential = Subject.doAs(subject, action);
        return (GSSCredential) credential;
    }

    private byte[] intAuthHandShake(byte[] pin, boolean[] done) throws SQLServerException {
        try {
            authLogger.log(Level.FINER, "{0}: Sending token to server over secure context", this);
            byte[] byteToken = peerContext.initSecContext(pin, 0, pin.length);

            if (peerContext.isEstablished()) {
                done[0] = true;
                authLogger.log(Level.FINER, "{0}: Authentication done", this);
            } else if (null == byteToken) {
                // The documentation is not clear on when this can happen but it does say this could happen
                authLogger.log(Level.FINER, "{0}: byteToken is null in initSecContext", this);

                con.terminate(SQLServerException.DRIVER_ERROR_NONE,
                        SQLServerException.getErrString("R_integratedAuthenticationFailed"));
            }
            return byteToken;
        } catch (GSSException ge) {
            LogUtil.finer(authLogger, "{0}: initSecContext Failed: {1}", this, ge);
            con.terminate(SQLServerException.DRIVER_ERROR_NONE,
                    SQLServerException.getErrString("R_integratedAuthenticationFailed"), ge);
        }
        // keep the compiler happy
        return null;
    }

    private String makeSpn(String server, int port) throws SQLServerException {
        LogUtil.finer(authLogger, "{0}: Server: {1} port: {2}", this, server, port);
        StringBuilder spn = new StringBuilder("MSSQLSvc/");
        // Format is MSSQLSvc/myhost.domain.company.com:1433
        // FQDN must be provided
        if (con.serverNameAsACE()) {
            spn.append(IDN.toASCII(server));
        } else {
            spn.append(server);
        }
        spn.append(":");
        spn.append(port);
        String strSPN = spn.toString();
        LogUtil.finer(authLogger, "{0}: SPN: {1}", this, strSPN);
        return strSPN;
    }

    // Package visible members below.
    KerbAuthentication(SQLServerConnection con, String address, int port) throws SQLServerException {
        this.con = con;
        // Get user provided SPN string; if not provided then build the generic one
        String userSuppliedServerSpn = con.activeConnectionProperties
                .getProperty(SQLServerDriverStringProperty.SERVER_SPN.toString());

        String spn;
        if (null != userSuppliedServerSpn) {
            // serverNameAsACE is true, translate the user supplied serverSPN to ASCII
            if (con.serverNameAsACE()) {
                int slashPos = userSuppliedServerSpn.indexOf("/");
                spn = userSuppliedServerSpn.substring(0, slashPos + 1)
                        + IDN.toASCII(userSuppliedServerSpn.substring(slashPos + 1));
            } else {
                spn = userSuppliedServerSpn;
            }
        } else {
            spn = makeSpn(address, port);
        }
        this.spn = enrichSpnWithRealm(spn, null == userSuppliedServerSpn);

        if (!this.spn.equals(spn)) {
            LogUtil.finer(authLogger, "{0}: SPN enriched: {1} := {2}", this, spn, this.spn);
        }
    }

    private static final Pattern SPN_PATTERN = Pattern.compile("MSSQLSvc/(.*):([^:@]+)(@.+)?",
            Pattern.CASE_INSENSITIVE);

    private String enrichSpnWithRealm(String spn, boolean allowHostnameCanonicalization) {
        if (spn == null) {
            return spn;
        }
        Matcher m = SPN_PATTERN.matcher(spn);
        if (!m.matches()) {
            return spn;
        }
        if (m.group(3) != null) {
            // Realm is already present, no need to enrich, the job has already been done
            return spn;
        }
        String dnsName = m.group(1);
        String portOrInstance = m.group(2);
        RealmValidator realmValidator = getRealmValidator();
        String realm = findRealmFromHostname(realmValidator, dnsName);
        if (realm == null && allowHostnameCanonicalization) {
            // We failed, try with canonical host name to find a better match
            try {
                String canonicalHostName = InetAddress.getByName(dnsName).getCanonicalHostName();
                realm = findRealmFromHostname(realmValidator, canonicalHostName);
                // Since we have a match, our hostname is the correct one (for instance of server
                // name was an IP), so we override dnsName as well
                dnsName = canonicalHostName;
            } catch (UnknownHostException cannotCanonicalize) {
                // ignored, but we are in a bad shape
            }
        }
        if (realm == null) {
            return spn;
        } else {
            StringBuilder sb = new StringBuilder("MSSQLSvc/");
            sb.append(dnsName).append(":").append(portOrInstance).append("@").append(realm.toUpperCase(Locale.ENGLISH));
            return sb.toString();
        }
    }

    private static RealmValidator validator;

    /**
     * Get validator to validate REALM for given JVM.
     *
     * @return a not null realm validator.
     */
    static RealmValidator getRealmValidator() {
        if (validator != null) {
            return validator;
        }

        validator = new RealmValidator() {
            @Override
            public boolean isRealmValid(String realm) {
                try {
                    return DNSKerberosLocator.isRealmValid(realm);
                } catch (NamingException err) {
                    return false;
                }
            }
        };
        return validator;
    }

    /**
     * Try to find a REALM in the different parts of a host name.
     *
     * @param realmValidator
     *        a function that return true if REALM is valid and exists
     * @param hostname
     *        the name we are looking a REALM for
     * @return the realm if found, null otherwise
     */
    private String findRealmFromHostname(RealmValidator realmValidator, String hostname) {
        if (hostname == null) {
            return null;
        }
        int index = 0;
        while (index != -1 && index < hostname.length() - 2) {
            String realm = hostname.substring(index);
            LogUtil.finest(authLogger, "{0}: looking up REALM candidate: {1}", this, realm);
            if (realmValidator.isRealmValid(realm)) {
                return realm.toUpperCase();
            }
            index = hostname.indexOf(".", index + 1);
            if (index != -1) {
                index = index + 1;
            }
        }
        return null;
    }

    /**
     * JVM Specific implementation to decide whether a realm is valid or not
     */
    interface RealmValidator {
        boolean isRealmValid(String realm);
    }

    /**
     * 
     * @param con
     * @param address
     * @param port
     * @param ImpersonatedUserCred
     * @throws SQLServerException
     */
    KerbAuthentication(SQLServerConnection con, String address, int port, GSSCredential ImpersonatedUserCred,
            Boolean isUserCreated) throws SQLServerException {
        this(con, address, port);
        peerCredentials = ImpersonatedUserCred;
        this.isUserCreatedCredential = (isUserCreated == null ? false : isUserCreated);
    }

    byte[] GenerateClientContext(byte[] pin, boolean[] done) throws SQLServerException {
        if (null == peerContext) {
            intAuthInit();
        }
        return intAuthHandShake(pin, done);
    }

    int ReleaseClientContext() throws SQLServerException {
        try {
            if (null != peerCredentials && !isUserCreatedCredential) {
                peerCredentials.dispose();
            } else if (null != peerCredentials && isUserCreatedCredential) {
                peerCredentials = null;
            }
            if (null != peerContext)
                peerContext.dispose();
            if (null != lc)
                lc.logout();
        } catch (LoginException e) {
            // yes we are eating exceptions here but this should not fail in the normal circumstances and we do not want
            // to eat previous
            // login errors if caused before which is more useful to the user than the cleanup errors.
            LogUtil.fine(authLogger, "{0}: Release of the credentials failed LoginException: {1}", this, e);
        } catch (GSSException e) {
            // yes we are eating exceptions here but this should not fail in the normal circumstances and we do not want
            // to eat previous
            // login errors if caused before which is more useful to the user than the cleanup errors.
            LogUtil.fine(authLogger, "{0}: Release of the credentials failed GSSException: {1}", this, e);
        }
        return 0;
    }
}
