/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import javax.security.auth.x500.X500Principal;
import org.junit.jupiter.api.Test;

/**
 * Tests for SSL certificate validation logic
 */
public class SSLCertificateValidationTest {

    /**
     * Tests our internal method, validateServerName() against different possible names in SSL certificate.
     * 
     * @throws Exception
     */
    @Test
    public void testValidateServerName() throws Exception {

        String serverName = "msjdbc.database.windows.net";
        String serverName2 = "bbbbuuzzuzzzzzz.example.net";
        String serverName3 = "xn--ms.database.windows.net";

        // Set up the HostNameOverrideX509TrustManager object using reflection
        TDSChannel tdsc = new TDSChannel(new SQLServerConnection("someConnectionProperty"));
        Class<?> hsoClass = Class.forName("com.microsoft.sqlserver.jdbc.HostNameOverrideX509TrustManager");
        Constructor<?> constructor = hsoClass.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        Object hsoObject = constructor.newInstance(tdsc, null, serverName);
        Method method = SQLServerCertificateUtils.class.getDeclaredMethod("validateServerName", String.class,
                String.class);
        method.setAccessible(true);

        /*
         * Server Name = msjdbc.database.windows.net SAN = msjdbc.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "msjdbc.database.windows.net", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = msjdbc***.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "msjdbc***.database.windows.net", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = ms*bc.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "ms*bc.database.windows.net", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *bc.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "*bc.database.windows.net", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = ms*.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "ms*.database.windows.net", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *jd*.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "*jd*.database.windows.net", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = ms.*.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "ms.*.net", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = msjdbc.asd*dsa.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "msjdbc.asd*dsa.windows.net", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *.*.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, ".*.windows.net", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = msjdbc.*.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "msjdbc.*.windows.net", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *.*.windows.net Expected result: false Note: multiple
         * wildcards are not allowed, so this case shouldn't happen, but we still make sure to fail this.
         */
        assertFalse((boolean) method.invoke(hsoObject, "*.*.windows.net", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *.com Expected result: false A cert with * plus a top-level
         * domain is not allowed.
         */
        assertFalse((boolean) method.invoke(hsoObject, "*.com", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = xn--ms*.database.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "xn--ms*.database.windows.net", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = * Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "*", serverName));

        /*
         * Server Name = msjdbc.database.windows.net SAN = ms*atabase.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "ms*atabase.windows.net", serverName));

        hsoObject = constructor.newInstance(tdsc, null, serverName2);

        /*
         * Server Name = bbbbuuzzuzzzzzz.example.net SAN = b*zzz.example.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "b*zzz.example.net", serverName2));

        hsoObject = constructor.newInstance(tdsc, null, serverName3);

        /*
         * Server Name = xn--ms.database.windows.net SAN = xn--ms.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "xn--ms.database.windows.net", serverName3));
    }

    // Minimal mock certificate for testing CN parsing
    private static X509Certificate mockCert(final X500Principal subject, final Collection<List<?>> sans) {
        return new X509Certificate() {
            public X500Principal getSubjectX500Principal() {
                return subject;
            }

            public Collection<List<?>> getSubjectAlternativeNames() {
                return sans;
            }

            public X500Principal getIssuerX500Principal() {
                return subject;
            }

            public java.security.Principal getSubjectDN() {
                return subject;
            }

            public java.security.Principal getIssuerDN() {
                return subject;
            }

            public void checkValidity() {
            }

            public void checkValidity(java.util.Date d) {
            }

            public int getVersion() {
                return 3;
            }

            public java.math.BigInteger getSerialNumber() {
                return java.math.BigInteger.ONE;
            }

            public java.util.Date getNotBefore() {
                return new java.util.Date();
            }

            public java.util.Date getNotAfter() {
                return new java.util.Date();
            }

            public byte[] getTBSCertificate() {
                return new byte[0];
            }

            public byte[] getSignature() {
                return new byte[0];
            }

            public String getSigAlgName() {
                return "";
            }

            public String getSigAlgOID() {
                return "";
            }

            public byte[] getSigAlgParams() {
                return null;
            }

            public boolean[] getIssuerUniqueID() {
                return null;
            }

            public boolean[] getSubjectUniqueID() {
                return null;
            }

            public boolean[] getKeyUsage() {
                return null;
            }

            public int getBasicConstraints() {
                return -1;
            }

            public byte[] getEncoded() {
                return new byte[0];
            }

            public void verify(java.security.PublicKey key) {
            }

            public void verify(java.security.PublicKey key, String sigProvider) {
            }

            public java.security.PublicKey getPublicKey() {
                return null;
            }

            public boolean hasUnsupportedCriticalExtension() {
                return false;
            }

            public java.util.Set<String> getCriticalExtensionOIDs() {
                return null;
            }

            public java.util.Set<String> getNonCriticalExtensionOIDs() {
                return null;
            }

            public byte[] getExtensionValue(String oid) {
                return null;
            }

            public String toString() {
                return "Mock Certificate";
            }
        };
    }

    @Test
    public void testSecureCNParsing_preventsHostnameSpoofing() throws Exception {
        // Certificate with spoofed CN via OU attribute: "OU=CN\=target.com,
        // CN=attacker.com"
        X500Principal spoofedSubject = new X500Principal("OU=CN\\=target.com, CN=attacker.com");
        X509Certificate spoofedCert = mockCert(spoofedSubject, null);

        // Should extract real CN (attacker.com), not the spoofed one (target.com)
        assertEquals("attacker.com", SQLServerCertificateUtils.parseCommonNameSecure(spoofedCert));

        // Should reject validation against spoofed hostname but allow real CN
        assertThrows(CertificateException.class,
                () -> SQLServerCertificateUtils.validateServerNameInCertificate(spoofedCert, "target.com"));
        assertDoesNotThrow(
                () -> SQLServerCertificateUtils.validateServerNameInCertificate(spoofedCert, "attacker.com"));
    }

    /**
     * Helper to create a SAN entry list.
     * Per X509Certificate.getSubjectAlternativeNames() spec:
     * - First element is an Integer representing the type (2 = dNSName, 7 = iPAddress)
     * - Second element is a String for dNSName and iPAddress types
     */
    private static List<?> sanEntry(int type, String value) {
        return Arrays.asList(Integer.valueOf(type), value);
    }

    @Test
    public void testIPAddressInSAN_IPv4Match() throws Exception {
        // Certificate with IPv4 address in SAN (type 7)
        X500Principal subject = new X500Principal("CN=sqlserver.example.com");
        Collection<List<?>> sans = new ArrayList<>();
        sans.add(sanEntry(7, "192.168.1.100"));

        X509Certificate cert = mockCert(subject, sans);

        // Should succeed when connecting via the IP address in SAN
        assertDoesNotThrow(
                () -> SQLServerCertificateUtils.validateServerNameInCertificate(cert, "192.168.1.100"));
    }

    @Test
    public void testIPAddressInSAN_IPv4Mismatch() throws Exception {
        // Certificate with IPv4 address in SAN (type 7)
        X500Principal subject = new X500Principal("CN=sqlserver.example.com");
        Collection<List<?>> sans = new ArrayList<>();
        sans.add(sanEntry(7, "192.168.1.100"));

        X509Certificate cert = mockCert(subject, sans);

        // Should fail when connecting via a different IP address
        assertThrows(CertificateException.class,
                () -> SQLServerCertificateUtils.validateServerNameInCertificate(cert, "192.168.1.101"));
    }

    @Test
    public void testIPAddressInSAN_IPv6Match() throws Exception {
        // Certificate with IPv6 address in SAN (type 7)
        X500Principal subject = new X500Principal("CN=sqlserver.example.com");
        Collection<List<?>> sans = new ArrayList<>();
        sans.add(sanEntry(7, "2001:db8::1"));

        X509Certificate cert = mockCert(subject, sans);

        // Should succeed when connecting via the IPv6 address in SAN
        assertDoesNotThrow(
                () -> SQLServerCertificateUtils.validateServerNameInCertificate(cert, "2001:db8::1"));
    }

    @Test
    public void testIPAddressInSAN_MultipleEntries() throws Exception {
        // Certificate with both DNS name and IP address in SAN
        X500Principal subject = new X500Principal("CN=sqlserver.example.com");
        Collection<List<?>> sans = new ArrayList<>();
        sans.add(sanEntry(2, "sqlserver.example.com"));
        sans.add(sanEntry(7, "10.0.0.50"));
        sans.add(sanEntry(7, "192.168.1.100"));

        X509Certificate cert = mockCert(subject, sans);

        // Should succeed for DNS name
        assertDoesNotThrow(
                () -> SQLServerCertificateUtils.validateServerNameInCertificate(cert, "sqlserver.example.com"));

        // Should succeed for first IP
        assertDoesNotThrow(
                () -> SQLServerCertificateUtils.validateServerNameInCertificate(cert, "10.0.0.50"));

        // Should succeed for second IP
        assertDoesNotThrow(
                () -> SQLServerCertificateUtils.validateServerNameInCertificate(cert, "192.168.1.100"));

        // Should fail for unlisted IP
        assertThrows(CertificateException.class,
                () -> SQLServerCertificateUtils.validateServerNameInCertificate(cert, "172.16.0.1"));
    }

    @Test
    public void testIPAddressInSAN_CaseInsensitive() throws Exception {
        // Certificate with IPv6 address in SAN - test case insensitivity
        X500Principal subject = new X500Principal("CN=sqlserver.example.com");
        Collection<List<?>> sans = new ArrayList<>();
        sans.add(sanEntry(7, "2001:DB8::1"));

        X509Certificate cert = mockCert(subject, sans);

        // Should succeed with different case (IPv6 addresses are case-insensitive)
        assertDoesNotThrow(
                () -> SQLServerCertificateUtils.validateServerNameInCertificate(cert, "2001:db8::1"));
    }

    @Test
    public void testIPAddressInSAN_FallbackToCN() throws Exception {
        // Certificate with only DNS SAN, no IP - should still work via CN fallback
        X500Principal subject = new X500Principal("CN=sqlserver.example.com");
        Collection<List<?>> sans = new ArrayList<>();
        sans.add(sanEntry(2, "sqlserver.example.com"));

        X509Certificate cert = mockCert(subject, sans);

        // Should succeed for hostname matching CN/SAN
        assertDoesNotThrow(
                () -> SQLServerCertificateUtils.validateServerNameInCertificate(cert, "sqlserver.example.com"));

        // Should fail for IP when no IP SAN is present
        assertThrows(CertificateException.class,
                () -> SQLServerCertificateUtils.validateServerNameInCertificate(cert, "192.168.1.100"));
    }
}
