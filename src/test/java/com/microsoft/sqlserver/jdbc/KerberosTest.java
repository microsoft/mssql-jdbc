package com.microsoft.sqlserver.jdbc;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;


@Tag(Constants.kerberos)
@RunWith(JUnitPlatform.class)
public class KerberosTest extends AbstractTest {

    private static String kerberosAuth = "KERBEROS";
    private static String authSchemeQuery = "select auth_scheme from sys.dm_exec_connections where session_id=@@spid";

    @BeforeAll
    public static void setupTests() throws Exception {
        Configuration.setConfiguration(new JaasConfiguration(Configuration.getConfiguration()));
        setConnection();
    }

    @Test
    public void testUseDefaultJaasConfigConnectionStringPropertyTrue() throws Exception {
        String connectionStringUseDefaultJaasConfig = connectionStringKerberos + ";useDefaultJaasConfig=true;";

        // Initial connection should succeed with default JAAS config
        createKerberosConnection(connectionStringUseDefaultJaasConfig);

        // Attempt to overwrite JAAS config. Since useDefaultJaasConfig=true, this should have no effect
        // and subsequent connections should succeed.
        overwriteJaasConfig();

        // New connection should successfully connect and continue to use the default JAAS config.
        createKerberosConnection(connectionStringUseDefaultJaasConfig);
    }

    @Test
    public void testUseDefaultJaasConfigConnectionStringPropertyFalse() throws Exception {

        // useDefaultJaasConfig=false by default
        // Initial connection should succeed with default JAAS config
        createKerberosConnection(connectionStringKerberos);

        // Overwrite JAAS config. Since useDefaultJaasConfig=false, overwriting should succeed and have an effect.
        // Subsequent connections will fail.
        overwriteJaasConfig();

        // New connection should fail as it is attempting to connect using an overwritten JAAS config.
        try {
            createKerberosConnection(connectionStringKerberos);
            Assertions.fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            Assertions.assertTrue(
                    e.getMessage().contains(TestResource.getResource("R_noLoginModulesConfiguredForJdbcDriver")));
        }
    }

    @Test
    public void testUseDefaultNativeGSSCredential() throws Exception {
        // This is a negative test. This test should fail as expected as the JVM arg "-Dsun.security.jgss.native=true"
        // isn't provided.
        String connectionString = connectionStringKerberos + ";useDefaultGSSCredential=true;";

        try {
            createKerberosConnection(connectionString);
            Assertions.fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            Assertions.assertEquals(e.getCause().getMessage(), TestResource.getResource("R_kerberosNativeGSSFailure"));
        }
    }

    @Test
    public void testBasicKerberosAuth() throws Exception {
        createKerberosConnection(connectionStringKerberos);
    }

    private static void createKerberosConnection(String connectionString) throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            ResultSet rs = conn.createStatement().executeQuery(authSchemeQuery);
            rs.next();
            Assertions.assertEquals(kerberosAuth, rs.getString(1));
        }
    }

    /**
     * Test to verify the Kerberos module used 
     */
    @Test
    public void testKerberosConnectionWithDefaultJaasConfig() {
        try {
            // Set a mock JAAS configuration using the existing method
            overwriteJaasConfig();

            String connectionString = connectionStringKerberos + ";useDefaultJaasConfig=true;";
            createKerberosConnection(connectionString);

            Configuration config = Configuration.getConfiguration();
            AppConfigurationEntry[] entries = config.getAppConfigurationEntry("CLIENT_CONTEXT_NAME");
            Assertions.assertNotNull(entries);
            Assertions.assertTrue(entries.length > 0);
            if (Util.isIBM()) {
                Assertions.assertEquals("com.ibm.security.auth.module.Krb5LoginModule", entries[0].getLoginModuleName());
            } else {
                Assertions.assertEquals("com.sun.security.auth.module.Krb5LoginModule", entries[0].getLoginModuleName());
            }
        } catch (Exception e) {
            Assertions.fail("Exception was thrown: " + e.getMessage());
        }
    }

    /**
     * Test to verify the JaasConfiguration constructor
     */
    @Test
    public void testJaasConfigurationConstructor() {
        try {
            JaasConfiguration config = new JaasConfiguration(Configuration.getConfiguration());
            Assertions.assertNotNull(config);
        } catch (SQLServerException e) {
            Assertions.fail("Exception was thrown: " + e.getMessage());
        }
    }

    /**
     * Test that assertSafeJaasLoginConfigProperty allows null/empty system property values.
     * When java.security.auth.login.config is unset, the method should pass without error.
     */
    @Test
    public void testAssertSafeJaasLoginConfigPropertyAllowsNullProperty() throws Exception {
        withJaasConfigProperty(null, assertMethod -> {
            // Null property => should pass
            assertMethod.invoke(null);
        });
    }

    /**
     * Test that assertSafeJaasLoginConfigProperty blocks remote URLs (http, https, ldap).
     */
    @Test
    public void testAssertSafeJaasLoginConfigPropertyRejectsRemoteUrls() throws Exception {

        String[] remoteUrls = {
                "http://remote.example.com/jaas.conf",
                "https://remote.example.com/jaas.conf",
                "ldap://remote.example.com/cn=config",
                "ftp://remote.example.com/jaas.conf"
        };

        for (String url : remoteUrls) {
            withJaasConfigProperty(url, assertMethod -> {
                try {
                    assertMethod.invoke(null);
                    Assertions.fail("Should have thrown for remote URL: " + url);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    Assertions.assertTrue(e.getCause() instanceof SQLServerException,
                            "Should throw SQLServerException for: " + url);
                }
            });
        }
    }

    /**
     * Test that assertSafeJaasLoginConfigProperty allows local file paths and file: URIs.
     */
    @Test
    public void testAssertSafeJaasLoginConfigPropertyAllowsLocalPaths() throws Exception {

        String[] localPaths = {
                "/etc/jaas.conf",
                "jaas.conf",
                "C:\\Users\\me\\jaas.conf",
                "C:/Users/me/jaas.conf",
                "file:///etc/jaas.conf",
                "file:///C:/Users/me/jaas.conf"
        };

        for (String path : localPaths) {
            withJaasConfigProperty(path, assertMethod -> {
                try {
                    assertMethod.invoke(null);
                    // Good - local path accepted
                } catch (java.lang.reflect.InvocationTargetException e) {
                    Assertions.fail("Should NOT have thrown for local path: " + path + " - " + e.getCause());
                }
            });
        }
    }

    /**
     * Regression test for MSRC-117029: proves the exact PoC payloads used in the
     * vulnerability report are now blocked by assertSafeJaasLoginConfigProperty.
     *
     * The original PoC sets java.security.auth.login.config to a remote HTTP URL
     * (with or without the '=' prefix that forces Java to reload the config).
     * The remote jaas.conf then declares JndiLoginModule with an LDAP/RMI URL,
     * triggering RCE via JNDI deserialization during lc.login().
     *
     * This test verifies the fix rejects such payloads at the property-validation
     * step, before any network or JNDI activity occurs.
     */
    @Test
    public void testMsrc117029PocPayloadsBlocked() throws Exception {

        // Exact payloads from the MSRC-117029 PoC reproduction scripts
        String[] pocPayloads = {
                "http://remote.example.com/evil.jaas",
                "=http://remote.example.com/evil.jaas",
                "https://remote.example.com/evil.jaas",
                "ldap://remote.example.com/cn=Evil",
                "rmi://remote.example.com/Exploit"
        };

        for (String payload : pocPayloads) {
            withJaasConfigProperty(payload, assertMethod -> {
                try {
                    assertMethod.invoke(null);
                    Assertions.fail("MSRC-117029 PoC NOT blocked for payload: " + payload);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    Assertions.assertTrue(e.getCause() instanceof SQLServerException,
                            "Should throw SQLServerException for PoC payload: " + payload);
                    Assertions.assertTrue(
                            e.getCause().getMessage().contains("must be a local file path"),
                            "Error message should indicate local file requirement for: " + payload);
                }
            });
        }
    }

    /**
     * Defense-in-depth test: malformed URIs that would throw URISyntaxException
     * but still start with a remote scheme prefix must be blocked.
     * Java's JAAS ConfigFile internally uses java.net.URL which is more lenient
     * than java.net.URI, so a value like "http://evil.com/path with spaces"
     * would fail URI parsing but could still be fetched by URL.
     */
    @Test
    public void testAssertSafeJaasLoginConfigPropertyRejectsMalformedRemoteUrls() throws Exception {

        // These are invalid URIs (would throw URISyntaxException) but start with remote schemes
        String[] malformedRemote = {"http://remote.example.com/path with spaces/evil.jaas",
                "https://remote.example.com/jaas config.conf", "ldap://remote.example.com/cn=foo bar",
                "rmi://remote.example.com/ex[ploit", "ftp://remote.example.com/file name.conf",
                "ldaps://remote.example.com/cn={bad}"};

        for (String payload : malformedRemote) {
            withJaasConfigProperty(payload, assertMethod -> {
                try {
                    assertMethod.invoke(null);
                    Assertions.fail("Should have blocked malformed remote URL: " + payload);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    Assertions.assertTrue(e.getCause() instanceof SQLServerException,
                            "Should throw SQLServerException for malformed remote URL: " + payload);
                }
            });
        }
    }

    /**
     * Test that assertSafeJaasLoginConfigProperty allows mounted drive paths,
     * UNC paths, mapped network drives, relative paths, paths with spaces,
     * and paths using the JAAS '=' prefix syntax.
     */
    @Test
    public void testAssertSafeJaasLoginConfigPropertyAllowsMountedAndPositiveFilePaths() throws Exception {

        String[] validPaths = {
                // Mounted drive paths
                "Z:\\shared\\security\\jaas.conf",
                "E:/mnt/kerberos/login.conf",
                "\\\\fileserver.domain.com\\security\\jaas.conf",
                // Positive file paths
                "../config/jaas.conf",
                "C:\\Program Files\\Java\\conf\\jaas.conf",
                "=/etc/jaas.conf",
                "=C:\\config\\jaas.conf"
        };

        for (String path : validPaths) {
            withJaasConfigProperty(path, assertMethod -> {
                try {
                    assertMethod.invoke(null);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    Assertions.fail("Should NOT have thrown for valid path: " + path + " - " + e.getCause());
                }
            });
        }
    }

    /**
     * Helper method to safely set and restore the java.security.auth.login.config property.
     * This reduces code duplication across multiple tests.
     *
     * @param propertyValue The value to set for java.security.auth.login.config (null to clear)
     * @param testBlock     The test logic to execute with the property set
     */
    private static void withJaasConfigProperty(String propertyValue,
            PropertyTestBlock testBlock) throws Exception {
        Class<?> kerbAuthClass = Class.forName("com.microsoft.sqlserver.jdbc.KerbAuthentication");
        java.lang.reflect.Method assertMethod = kerbAuthClass.getDeclaredMethod("assertSafeJaasLoginConfigProperty");
        assertMethod.setAccessible(true);

        String original = System.getProperty("java.security.auth.login.config");
        try {
            if (propertyValue == null) {
                System.clearProperty("java.security.auth.login.config");
            } else {
                System.setProperty("java.security.auth.login.config", propertyValue);
            }
            testBlock.execute(assertMethod);
        } finally {
            if (original != null) {
                System.setProperty("java.security.auth.login.config", original);
            } else {
                System.clearProperty("java.security.auth.login.config");
            }
        }
    }

    @FunctionalInterface
    private interface PropertyTestBlock {
        void execute(java.lang.reflect.Method assertMethod) throws Exception;
    }

    /**
     * Overwrites the default JAAS config. Call before making a connection.
     */
    private static void overwriteJaasConfig() {
        AppConfigurationEntry kafkaClientConfigurationEntry = new AppConfigurationEntry(
                "com.sun.security.auth.module.Krb5LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                new HashMap<>());
        Map<String, AppConfigurationEntry[]> configurationEntries = new HashMap<>();
        configurationEntries.put("CLIENT_CONTEXT_NAME", new AppConfigurationEntry[] {kafkaClientConfigurationEntry});
        Configuration.setConfiguration(new InternalConfiguration(configurationEntries));
    }

    private static class InternalConfiguration extends Configuration {
        private final Map<String, AppConfigurationEntry[]> configurationEntries;

        InternalConfiguration(Map<String, AppConfigurationEntry[]> configurationEntries) {
            this.configurationEntries = configurationEntries;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            return this.configurationEntries.get(name);
        }
    }
}
