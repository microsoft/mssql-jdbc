/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

import javax.security.auth.x500.X500Principal;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;


@RunWith(JUnitPlatform.class)
public class SSLCertificateValidation {

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
        Class<?> hsoClass = Class.forName("com.microsoft.sqlserver.jdbc.TDSChannel$HostNameOverrideX509TrustManager");
        Constructor<?> constructor = hsoClass.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        Object hsoObject = constructor.newInstance(null, tdsc, null, serverName);
        Method method = hsoObject.getClass().getDeclaredMethod("validateServerName", String.class);
        method.setAccessible(true);

        /*
         * Server Name = msjdbc.database.windows.net SAN = msjdbc.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "msjdbc.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = msjdbc***.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "msjdbc***.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = ms*bc.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "ms*bc.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *bc.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "*bc.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = ms*.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "ms*.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *jd*.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "*jd*.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = ms.*.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "ms.*.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = msjdbc.asd*dsa.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "msjdbc.asd*dsa.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *.*.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, ".*.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = msjdbc.*.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "msjdbc.*.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *.*.windows.net Expected result: false Note: multiple
         * wildcards are not allowed, so this case shouldn't happen, but we still make sure to fail this.
         */
        assertFalse((boolean) method.invoke(hsoObject, "*.*.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = *.com Expected result: false A cert with * plus a top-level
         * domain is not allowed.
         */
        assertFalse((boolean) method.invoke(hsoObject, "*.com"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = xn--ms*.database.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "xn--ms*.database.windows.net"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = * Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "*"));

        /*
         * Server Name = msjdbc.database.windows.net SAN = ms*atabase.windows.net Expected result: false
         */
        assertFalse((boolean) method.invoke(hsoObject, "ms*atabase.windows.net"));

        hsoObject = constructor.newInstance(null, tdsc, null, serverName2);
        method = hsoObject.getClass().getDeclaredMethod("validateServerName", String.class);
        method.setAccessible(true);

        /*
         * Server Name = bbbbuuzzuzzzzzz.example.net SAN = b*zzz.example.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "b*zzz.example.net"));

        hsoObject = constructor.newInstance(null, tdsc, null, serverName3);
        method = hsoObject.getClass().getDeclaredMethod("validateServerName", String.class);
        method.setAccessible(true);

        /*
         * Server Name = xn--ms.database.windows.net SAN = xn--ms.database.windows.net Expected result: true
         */
        assertTrue((boolean) method.invoke(hsoObject, "xn--ms.database.windows.net"));
    }

    // Minimal mock certificate for testing CN parsing security
    private static X509Certificate mockCert(final X500Principal subject, final Collection<List<?>> sans) {
        return new X509Certificate() {
            public X500Principal getSubjectX500Principal() { return subject; }
            public Collection<List<?>> getSubjectAlternativeNames() { return sans; }
            public X500Principal getIssuerX500Principal() { return subject; }
            public java.security.Principal getSubjectDN() { return subject; }
            public java.security.Principal getIssuerDN() { return subject; }
            public void checkValidity() {}
            public void checkValidity(java.util.Date d) {}
            public int getVersion() { return 3; }
            public java.math.BigInteger getSerialNumber() { return java.math.BigInteger.ONE; }
            public java.util.Date getNotBefore() { return new java.util.Date(); }
            public java.util.Date getNotAfter() { return new java.util.Date(); }
            public byte[] getTBSCertificate() { return new byte[0]; }
            public byte[] getSignature() { return new byte[0]; }
            public String getSigAlgName() { return ""; }
            public String getSigAlgOID() { return ""; }
            public byte[] getSigAlgParams() { return null; }
            public boolean[] getIssuerUniqueID() { return null; }
            public boolean[] getSubjectUniqueID() { return null; }
            public boolean[] getKeyUsage() { return null; }
            public int getBasicConstraints() { return -1; }
            public byte[] getEncoded() { return new byte[0]; }
            public void verify(java.security.PublicKey key) {}
            public void verify(java.security.PublicKey key, String sigProvider) {}
            public java.security.PublicKey getPublicKey() { return null; }
            public boolean hasUnsupportedCriticalExtension() { return false; }
            public java.util.Set<String> getCriticalExtensionOIDs() { return null; }
            public java.util.Set<String> getNonCriticalExtensionOIDs() { return null; }
            public byte[] getExtensionValue(String oid) { return null; }
            public String toString() { return "Mock Certificate"; }
        };
    }

    @Test
    public void testSecureCNParsing_preventsHostnameSpoofing() throws Exception {
        // Certificate with spoofed CN via OU attribute: "OU=CN\=target.com, CN=attacker.com"
        X500Principal spoofedSubject = new X500Principal("OU=CN\\=target.com, CN=attacker.com");
        X509Certificate spoofedCert = mockCert(spoofedSubject, null);
        
        // Set up the HostNameOverrideX509TrustManager object using reflection
        TDSChannel tdsc = new TDSChannel(new SQLServerConnection("someConnectionProperty"));
        Class<?> hsoClass = Class.forName("com.microsoft.sqlserver.jdbc.TDSChannel$HostNameOverrideX509TrustManager");
        Constructor<?> constructor = hsoClass.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        
        // Test rejection against spoofed hostname
        Object hsoObjectTargetCom = constructor.newInstance(null, tdsc, null, "target.com");
        Method validateMethod = hsoClass.getDeclaredMethod("validateServerNameInCertificate", X509Certificate.class);
        validateMethod.setAccessible(true);
        
        // Should throw exception when validating against spoofed hostname
        // Note: Method.invoke() wraps exceptions in InvocationTargetException, so we need to unwrap it
        try {
            validateMethod.invoke(hsoObjectTargetCom, spoofedCert);
            throw new AssertionError("Expected CertificateException to be thrown");
        } catch (java.lang.reflect.InvocationTargetException e) {
            // Unwrap and verify it's a CertificateException
            assertTrue("Expected CertificateException but got: " + e.getCause().getClass().getName(),
                e.getCause() instanceof CertificateException);
        }
        
        // Should pass when validating against real CN
        Object hsoObjectAttackerCom = constructor.newInstance(null, tdsc, null, "attacker.com");
        assertDoesNotThrow(() -> {
            validateMethod.invoke(hsoObjectAttackerCom, spoofedCert);
        });
    }
}
