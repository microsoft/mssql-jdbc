/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import javax.naming.Reference;
import javax.naming.StringRefAddr;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;


/**
 * Unit tests verifying that {@link SQLServerDataSource#getReference()} does not expose sensitive credential properties
 * in the returned JNDI {@link Reference}. This is a regression test for CWE-200 information exposure.
 */
@Tag("Unit")
public class SQLServerDataSourceReferenceSecurityTest {

    /**
     * Verifies that none of the credential-bearing properties leak into the JNDI Reference. Sets every known sensitive
     * property, calls getReference(), and asserts they are all omitted.
     */
    @Test
    public void testSensitivePropertiesNotExposedInReference() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();

        // Set non-sensitive properties that SHOULD appear.
        ds.setServerName("myserver.database.windows.net");
        ds.setDatabaseName("mydb");
        ds.setUser("admin");

        // Set all sensitive properties.
        ds.setPassword("P@ssw0rd!");
        ds.setAccessToken("eyJhbGciOiJSUzI1...");
        ds.setKeyStoreSecret("keystoreSecretValue");
        ds.setKeyVaultProviderClientKey("vaultClientKeyValue");
        ds.setClientKeyPassword("clientKeyPwdValue");
        ds.setAADSecurePrincipalSecret("aadSecretValue");

        Reference ref = ds.getReference();
        assertNotNull(ref, "Reference must not be null");

        // Collect all property names in the reference.
        Set<String> refPropertyNames = new HashSet<>();
        Enumeration<?> addrs = ref.getAll();
        while (addrs.hasMoreElements()) {
            StringRefAddr addr = (StringRefAddr) addrs.nextElement();
            refPropertyNames.add(addr.getType());
        }

        // Verify non-sensitive properties ARE present.
        assertTrue(refPropertyNames.contains("serverName"),
                "Non-sensitive property 'serverName' should be in Reference");
        assertTrue(refPropertyNames.contains("databaseName"),
                "Non-sensitive property 'databaseName' should be in Reference");
        assertTrue(refPropertyNames.contains("user"), "Non-sensitive property 'user' should be in Reference");

        // Verify sensitive properties are NOT present.
        Set<String> sensitiveProps = new HashSet<>();
        sensitiveProps.add("password");
        sensitiveProps.add("accessToken");
        sensitiveProps.add("keyStoreSecret");
        sensitiveProps.add("keyVaultProviderClientKey");
        sensitiveProps.add("clientKeyPassword");
        sensitiveProps.add("AADSecurePrincipalSecret");

        for (String sensitive : sensitiveProps) {
            assertNull(ref.get(sensitive),
                    "Sensitive property '" + sensitive + "' must NOT appear in the JNDI Reference");
        }
    }

    /**
     * Verifies that properties whose names end with "password" or "secret" are omitted via suffix matching even if they
     * are not in the explicit denylist. This tests the defense-in-depth suffix logic by setting connection properties
     * directly via the URL mechanism is not feasible (URL is stored separately), so we verify via the known properties
     * that already exercise the suffix path: clientKeyPassword ends with "password" and AADSecurePrincipalSecret ends
     * with "Secret".
     */
    @Test
    public void testSuffixBasedSensitivePropertyOmission() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName("myserver");

        // clientKeyPassword ends with "password" -- suffix match
        ds.setClientKeyPassword("suffixPasswordTest");
        // AADSecurePrincipalSecret ends with "Secret" -- suffix match
        ds.setAADSecurePrincipalSecret("suffixSecretTest");

        Reference ref = ds.getReference();
        assertNotNull(ref, "Reference must not be null");

        // Verify serverName (non-sensitive) is present.
        assertNotNull(ref.get("serverName"), "'serverName' should be in Reference");

        // Verify sensitive properties are NOT present.
        assertNull(ref.get("clientKeyPassword"),
                "Property ending in 'password' must be omitted from Reference");
        assertNull(ref.get("AADSecurePrincipalSecret"),
                "Property ending in 'Secret' must be omitted from Reference");

        // Also verify the values don't appear in any other property.
        Enumeration<?> addrs = ref.getAll();
        while (addrs.hasMoreElements()) {
            StringRefAddr addr = (StringRefAddr) addrs.nextElement();
            String content = (String) addr.getContent();
            if (content != null) {
                assertTrue(!content.contains("suffixPasswordTest"),
                        "Value from a *password property must not appear in Reference");
                assertTrue(!content.contains("suffixSecretTest"),
                        "Value from a *secret property must not appear in Reference");
            }
        }
    }
}
