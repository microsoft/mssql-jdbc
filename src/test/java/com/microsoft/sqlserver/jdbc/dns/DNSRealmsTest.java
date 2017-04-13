/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.dns;

import javax.naming.NamingException;

public class DNSRealmsTest {

    public static void main(String... args) {
        if (args.length < 1) {
            System.err.println("USAGE: list of domains to test for kerberos realms");
        }
        for (String realmName : args) {
            try {
                System.out.print(DNSKerberosLocator.isRealmValid(realmName) ? "[ VALID ] " : "[INVALID] ");
            } catch (NamingException err) {
                System.err.print("[ FAILED] : " + err.getClass().getName() + ":" + err.getMessage());
            }
            System.out.println(realmName);
        }
    }

}
