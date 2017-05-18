/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.dns;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Describe an DNS SRV Record.
 */
public class DNSRecordSRV implements Comparable<DNSRecordSRV> {

    private static final Pattern PATTERN = Pattern.compile("^([0-9]+) ([0-9]+) ([0-9]+) (.+)$");

    private final int priority;

    /**
     * Parse a DNS SRC Record from a DNS String record.
     *
     * @param record
     *            the record to parse
     * @return a not null DNS Record
     * @throws IllegalArgumentException
     *             if record is not correct and cannot be parsed
     */
    public static DNSRecordSRV parseFromDNSRecord(String record) throws IllegalArgumentException {
        Matcher m = PATTERN.matcher(record);
        if (!m.matches()) {
            throw new IllegalArgumentException("record '" + record + "' cannot be matched as a valid DNS SRV Record");
        }
        try {
            int priority = Integer.parseInt(m.group(1));
            int weight = Integer.parseInt(m.group(2));
            int port = Integer.parseInt(m.group(3));
            String serverName = m.group(4);
            // Avoid issues with Kerberos SPN when fully qualified records ends with '.'
            if (serverName.endsWith(".")) {
                serverName = serverName.substring(0, serverName.length() - 1);
            }
            return new DNSRecordSRV(priority, weight, port, serverName);
        }
        catch (IllegalArgumentException err) {
            throw err;
        }
        catch (Exception err) {
            throw new IllegalArgumentException("Failed to parse DNS SRV record '" + record + "'", err);
        }
    }

    @Override
    public String toString() {
        return String.format("DNS.SRV[pri=%d w=%d port=%d h='%s']", priority, weight, port, serverName);
    }

    /**
     * Constructor.
     *
     * @param priority
     *            is lowest
     * @param weight
     *            1 at minimum
     * @param port
     *            the port of service
     * @param serverName
     *            the host
     * @throws IllegalArgumentException
     *             if priority {@literal <} 0 or weight {@literal <=} 1
     */
    public DNSRecordSRV(int priority,
            int weight,
            int port,
            String serverName) throws IllegalArgumentException {
        if (priority < 0) {
            throw new IllegalArgumentException("priority must be >= 0, but was: " + priority);
        }
        this.priority = priority;
        if (weight < 0) {
            // Weight == 0 is OK to disable load balancing, but not below
            throw new IllegalArgumentException("weight must be >= 0, but was: " + weight);
        }
        this.weight = weight;
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535, but was: " + port);
        }
        this.port = port;
        if (serverName == null || serverName.trim().isEmpty()) {
            throw new IllegalArgumentException("hostname is not supposed to be null or empty in a SRV Record");
        }
        this.serverName = serverName;
    }

    private final int weight;
    private final int port;
    private final String serverName;

    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof DNSRecordSRV)) {
            return false;
        }

        DNSRecordSRV r = (DNSRecordSRV) other;
        return port == r.port && weight == r.weight && priority == r.priority && serverName.equals(r.serverName);
    }

    @Override
    public int compareTo(DNSRecordSRV o) {
        if (o == null) {
            return 1;
        }
        int p = Integer.compare(priority, o.priority);
        if (p != 0) {
            return p;
        }
        p = Integer.compare(weight, o.weight);
        if (p != 0) {
            return p;
        }
        p = Integer.compare(port, o.port);
        if (p != 0) {
            return p;
        }
        return serverName.compareTo(o.serverName);
    }

    /**
     * Get the priority of DNS SRV record.
     * @return a positive priority, where lowest values have to be considered first.
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Get the weight of DNS record from 0 to 65535.
     * @return The weight, higher value means higher probability of selecting the given record for a given priority.
     */
    public int getWeight() {
        return weight;
    }

    /**
     * IP port of record.
     * @return a value from 1 to 65535.
     */
    public int getPort() {
        return port;
    }

    /**
     * The DNS server name.
     * @return a not null server name.
     */
    public String getServerName() {
        return serverName;
    }
}
