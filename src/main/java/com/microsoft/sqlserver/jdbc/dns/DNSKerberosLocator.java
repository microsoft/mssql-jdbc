/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.dns;

import java.util.Set;

import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

public final class DNSKerberosLocator {

    private DNSKerberosLocator() {
    }

    /**
     * Tells whether a realm is valid.
     *
     * @param realmName
     *            the realm to test
     * @return true if realm is valid, false otherwise
     * @throws NamingException
     *             if DNS failed, so realm existence cannot be determined
     */
    public static boolean isRealmValid(String realmName) throws NamingException {
        if (realmName == null || realmName.length() < 2) {
            return false;
        }
        if (realmName.startsWith(".")) {
            realmName = realmName.substring(1);
        }
        try {
            Set<DNSRecordSRV> records = DNSUtilities.findSrvRecords("_kerberos._udp." + realmName);
            return !records.isEmpty();
        }
        catch (NameNotFoundException wrongDomainException) {
            return false;
        }
    }
}
