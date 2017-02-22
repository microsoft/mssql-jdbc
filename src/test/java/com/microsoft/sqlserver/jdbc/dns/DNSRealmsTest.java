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
