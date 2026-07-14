/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.unit.datasource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import javax.naming.Reference;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

/**
 * Unit tests for {@link SQLServerDataSource#getReference()} credential-redaction behavior.
 *
 * Verifies that credential-bearing connection properties are never serialized as plaintext into the JNDI
 * {@link Reference} returned by {@code getReference()}, while non-sensitive properties are preserved.
 *
 * These tests require no SQL Server connection.
 */
@RunWith(JUnitPlatform.class)
@DisplayName("SQLServerDataSource.getReference() credential redaction tests")
public class DataSourceGetReferenceTest {

    /**
     * Verifies that all known sensitive credential properties are absent from the Reference, while a non-sensitive
     * property (serverName) is present.
     */
    @Test
    @DisplayName("Sensitive credential properties must not appear in Reference")
    public void testSensitivePropertiesAbsentFromReference() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName("test-server");
        ds.setPassword("test-password");
        ds.setAccessToken("test-access-token");
        ds.setKeyStoreSecret("test-key-store-secret");
        ds.setKeyVaultProviderClientKey("test-key-vault-client-key");
        ds.setClientKeyPassword("test-client-key-password");
        ds.setAADSecurePrincipalSecret("test-aad-principal-secret");

        Reference ref = ds.getReference();

        // password must be absent
        assertNull(ref.get("password"),
                "password must not appear in Reference");

        // accessToken must be absent
        assertNull(ref.get("accessToken"),
                "accessToken must not appear in Reference — bearer token exposure");

        // keyStoreSecret must be absent
        assertNull(ref.get("keyStoreSecret"),
                "keyStoreSecret must not appear in Reference");

        // keyVaultProviderClientKey must be absent
        assertNull(ref.get("keyVaultProviderClientKey"),
                "keyVaultProviderClientKey must not appear in Reference");

        // clientKeyPassword must be absent (capital P in name, was missed by old case-sensitive filter)
        assertNull(ref.get("clientKeyPassword"),
                "clientKeyPassword must not appear in Reference — was leaked by case-sensitive substring filter");

        // AADSecurePrincipalSecret must be absent
        assertNull(ref.get("AADSecurePrincipalSecret"),
                "AADSecurePrincipalSecret must not appear in Reference");

        // trustStorePassword must be absent (covered by existing stripped-marker behavior)
        assertNull(ref.get("trustStorePassword"),
                "trustStorePassword must not appear in Reference");

        // Non-sensitive property must still be present
        assertNotNull(ref.get("serverName"),
                "serverName must remain in Reference — it is not sensitive");
    }

    /**
     * Verifies that trustStorePassword omission uses the stripped-marker mechanism and that the marker is present when
     * trustStorePassword is set.
     */
    @Test
    @DisplayName("trustStorePassword stripped-marker present when trustStorePassword is set")
    public void testTrustStorePasswordStrippedMarkerPresent() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setTrustStorePassword("test-trust-store-password");

        Reference ref = ds.getReference();

        // value must be absent
        assertNull(ref.get("trustStorePassword"),
                "trustStorePassword value must not appear in Reference");

        // marker must be present
        assertNotNull(ref.get("trustStorePasswordStripped"),
                "trustStorePasswordStripped marker must be present when trustStorePassword is configured");
    }

    /**
     * Verifies that a data source with no secrets configured still produces a usable Reference containing the
     * non-sensitive properties.
     */
    @Test
    @DisplayName("Reference contains non-sensitive properties when no secrets are configured")
    public void testNonSensitivePropertiesRetained() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName("prod-server");
        ds.setPortNumber(1433);
        ds.setDatabaseName("myDatabase");

        Reference ref = ds.getReference();

        assertNotNull(ref.get("serverName"), "serverName must be in Reference");
        assertNotNull(ref.get("portNumber"), "portNumber must be in Reference");
        assertNotNull(ref.get("databaseName"), "databaseName must be in Reference");
    }
}
