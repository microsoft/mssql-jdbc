/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.net.IDN;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.NamingException;

import com.microsoft.sqlserver.jdbc.dns.DNSKerberosLocator;


/**
 * Integrated Authentication master file. Common items for kerb and JNI auth are in this interface.
 */

abstract class SSPIAuthentication {
    abstract byte[] generateClientContext(byte[] pin, boolean[] done) throws SQLServerException;

    abstract void releaseClientContext();

    /**
     * SPN pattern for matching
     */
    private static final Pattern SPN_PATTERN = Pattern.compile("MSSQLSvc/(.*):([^:@]+)(@.+)?",
            Pattern.CASE_INSENSITIVE);

    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.SSPIAuthentication");

    /**
     * Make SPN name
     * 
     * @param con
     *        connection to SQL server
     * @param server
     *        server name
     * @param port
     *        port number
     * @return SPN
     */
    private String makeSpn(SQLServerConnection con, String server, int port) {
        StringBuilder spn = new StringBuilder("MSSQLSvc/");
        // Format is MSSQLSvc/myhost.domain.company.com:1433 FQDN must be provided
        if (con.serverNameAsACE()) {
            spn.append(IDN.toASCII(server));
        } else {
            spn.append(server);
        }
        spn.append(":");
        spn.append(port);
        return spn.toString();
    }

    /**
     * JVM Specific implementation to decide whether a realm is valid or not
     */
    interface RealmValidator {
        boolean isRealmValid(String realm);
    }

    private RealmValidator validator;

    /**
     * Get validator to validate REALM for given JVM.
     *
     * @return a not null realm validator
     */
    private RealmValidator getRealmValidator() {
        if (null != validator) {
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
            if (realmValidator.isRealmValid(realm)) {
                return realm.toUpperCase();
            }
            index = hostname.indexOf('.', index + 1);
            if (-1 != index) {
                index = index + 1;
            }
        }
        return null;
    }

    /**
     * Enrich SPN with Realm
     * 
     * @param spn
     *        SPN
     * @param allowHostnameCanonicalization
     *        flag to indicate of hostname canonicalization is allowed
     * @return SPN enriched with realm
     */
    String enrichSpnWithRealm(SQLServerConnection con, String spn, boolean allowHostnameCanonicalization) {
        if (spn == null) {
            return spn;
        }
        Matcher m = SPN_PATTERN.matcher(spn);
        if (!m.matches()) {
            return spn;
        }
        String realm = con.activeConnectionProperties.getProperty(SQLServerDriverStringProperty.REALM.toString());
        if (m.group(3) != null && (null == realm || realm.trim().isEmpty())) {
            // Realm is already present, no need to enrich, the job has already been done
            return spn;
        }

        // Try to derive realm if not specified in the connection. This might take some time if DNS lookup is slow
        if (logger.isLoggable(Level.FINER)) {
            logger.finer("Deriving realm");
        }

        String dnsName = m.group(1);
        String portOrInstance = m.group(2);
        try {
            if (null == realm || realm.trim().isEmpty()) {
                RealmValidator realmValidator = getRealmValidator();
                realm = findRealmFromHostname(realmValidator, dnsName);
                if (null == realm && allowHostnameCanonicalization) {
                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer("Attempt to derive realm using canonical host name with InetAddress");
                    }
                    // We failed, try with canonical host name to find a better match
                    String canonicalHostName = InetAddress.getByName(dnsName).getCanonicalHostName();
                    realm = findRealmFromHostname(realmValidator, canonicalHostName);
                    // Match means hostname is correct (eg if server name was an IP) so override dnsName as well
                    dnsName = canonicalHostName;
                }
            } else {
                if (allowHostnameCanonicalization) {
                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer(
                                "Since realm is provided, try to resolve canonical host name to host name with InetAddress");
                    }
                    // Realm was provided, try to resolve cname to hostname
                    dnsName = InetAddress.getByName(dnsName).getCanonicalHostName();
                }
            }
        } catch (UnknownHostException e) {
            // Ignored, cannot canonicalize
            if (logger.isLoggable(Level.FINER)) {
                logger.finer("Could not canonicalize host name. " + e.toString());
            }
        }

        if (null == realm) {
            if (logger.isLoggable(Level.FINER)) {
                logger.finer("Could not derive realm.");
            }
            return spn;
        } else {
            if (logger.isLoggable(Level.FINER)) {
                logger.finer("Derived realm: " + realm);
            }
            StringBuilder sb = new StringBuilder("MSSQLSvc/");
            sb.append(dnsName).append(":").append(portOrInstance).append("@").append(realm.toUpperCase(Locale.ENGLISH));
            return sb.toString();
        }
    }

    /**
     * Get SPN from connection string if provided or build a generic one
     * 
     * @param con
     *        connection to SQL server
     * @return SPN
     */
    String getSpn(SQLServerConnection con) {
        if (null == con || null == con.activeConnectionProperties) {
            return null;
        }

        String spn;
        String userSuppliedServerSpn = con.activeConnectionProperties
                .getProperty(SQLServerDriverStringProperty.SERVER_SPN.toString());
        if (null != userSuppliedServerSpn) {
            // serverNameAsACE is true, translate the user supplied serverSPN to ASCII
            if (con.serverNameAsACE()) {
                int slashPos = userSuppliedServerSpn.indexOf('/');
                spn = userSuppliedServerSpn.substring(0, slashPos + 1)
                        + IDN.toASCII(userSuppliedServerSpn.substring(slashPos + 1));
            } else {
                spn = userSuppliedServerSpn;
            }
        } else {
            spn = makeSpn(con, con.currentConnectPlaceHolder.getServerName(),
                    con.currentConnectPlaceHolder.getPortNumber());
        }
        return enrichSpnWithRealm(con, spn, null == userSuppliedServerSpn);
    }
}
