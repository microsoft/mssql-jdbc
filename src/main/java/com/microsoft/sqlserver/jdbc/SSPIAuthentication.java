/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.net.IDN;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
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
            index = hostname.indexOf(".", index + 1);
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
    String enrichSpnWithRealm(String spn, boolean allowHostnameCanonicalization) {
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
                // match means hostname is correct (for instance if server name was an IP) so override dnsName as well
                dnsName = canonicalHostName;
            } catch (UnknownHostException e) {
                // ignored, cannot canonicalize
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
                int slashPos = userSuppliedServerSpn.indexOf("/");
                spn = userSuppliedServerSpn.substring(0, slashPos + 1)
                        + IDN.toASCII(userSuppliedServerSpn.substring(slashPos + 1));
            } else {
                spn = userSuppliedServerSpn;
            }
        } else {
            spn = makeSpn(con, con.currentConnectPlaceHolder.getServerName(),
                    con.currentConnectPlaceHolder.getPortNumber());
        }
        return enrichSpnWithRealm(spn, null == userSuppliedServerSpn);
    }
}
