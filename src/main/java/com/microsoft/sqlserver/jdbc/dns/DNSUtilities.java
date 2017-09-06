/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.dns;

import java.util.Hashtable;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

public class DNSUtilities {

    private final static Logger LOG = Logger.getLogger(DNSUtilities.class.getName());

    private static final Level DNS_ERR_LOG_LEVEL = Level.FINE;

    /**
     * Find all SRV Record using DNS.
     *
     * @param dnsSrvRecordToFind
     *            the DNS record, for instance: _ldap._tcp.dc._msdcs.DOMAIN.COM to find all LDAP servers in DOMAIN.COM
     * @return the collection of records with facilities to find the best candidate
     * @throws NamingException
     *             if DNS is not available
     */
    public static Set<DNSRecordSRV> findSrvRecords(final String dnsSrvRecordToFind) throws NamingException {
        Hashtable<Object, Object> env = new Hashtable<>();
        env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
        env.put("java.naming.provider.url", "dns:");
        DirContext ctx = new InitialDirContext(env);
        Attributes attrs = ctx.getAttributes(dnsSrvRecordToFind, new String[] {"SRV"});
        NamingEnumeration<? extends Attribute> allServers = attrs.getAll();
        TreeSet<DNSRecordSRV> records = new TreeSet<>();
        while (allServers.hasMoreElements()) {
            Attribute a = allServers.nextElement();
            NamingEnumeration<?> srvRecord = a.getAll();
            while (srvRecord.hasMore()) {
                final String record = String.valueOf(srvRecord.nextElement());
                try {
                    DNSRecordSRV rec = DNSRecordSRV.parseFromDNSRecord(record);
                    if (rec != null) {
                        records.add(rec);
                    }
                }
                catch (IllegalArgumentException errorParsingRecord) {
                    if (LOG.isLoggable(DNS_ERR_LOG_LEVEL)) {
                        LOG.log(DNS_ERR_LOG_LEVEL, String.format("Failed to parse SRV DNS Record: '%s'", record), errorParsingRecord);
                    }
                }
            }
            srvRecord.close();
        }
        allServers.close();
        return records;
    }
}
