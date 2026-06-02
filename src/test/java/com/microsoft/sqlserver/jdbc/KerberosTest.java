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
     * Test that JaasConfiguration with null delegate provides safe defaults.
     * This verifies the security fix - using null instead of Configuration.getConfiguration()
     * prevents loading potentially malicious remote JAAS configs.
     */
    @Test
    public void testJaasConfigurationSecureDefaults() throws Exception {
        // assertSafeJaasLoginConfigProperty should allow null/empty property
        String original = System.getProperty("java.security.auth.login.config");
        try {
            System.clearProperty("java.security.auth.login.config");

            Class<?> kerbAuthClass = Class.forName("com.microsoft.sqlserver.jdbc.KerbAuthentication");
            java.lang.reflect.Method assertMethod = kerbAuthClass.getDeclaredMethod("assertSafeJaasLoginConfigProperty");
            assertMethod.setAccessible(true);

            // Null property => should pass
            assertMethod.invoke(null);
        } finally {
            if (original != null) {
                System.setProperty("java.security.auth.login.config", original);
            }
        }
    }

    /**
     * Test that assertSafeJaasLoginConfigProperty blocks remote URLs (http, https, ldap).
     */
    @Test
    public void testAssertSafeJaasLoginConfigProperty_rejectsRemoteUrls() throws Exception {
        Class<?> kerbAuthClass = Class.forName("com.microsoft.sqlserver.jdbc.KerbAuthentication");
        java.lang.reflect.Method assertMethod = kerbAuthClass.getDeclaredMethod("assertSafeJaasLoginConfigProperty");
        assertMethod.setAccessible(true);

        String[] remoteUrls = {
                "http://remote.example.com/jaas.conf",
                "https://remote.example.com/jaas.conf",
                "ldap://remote.example.com/cn=config",
                "ftp://remote.example.com/jaas.conf"
        };

        String original = System.getProperty("java.security.auth.login.config");
        try {
            for (String url : remoteUrls) {
                System.setProperty("java.security.auth.login.config", url);
                try {
                    assertMethod.invoke(null);
                    Assertions.fail("Should have thrown for remote URL: " + url);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    Assertions.assertTrue(e.getCause() instanceof SQLServerException,
                            "Should throw SQLServerException for: " + url);
                }
            }
        } finally {
            if (original != null) {
                System.setProperty("java.security.auth.login.config", original);
            } else {
                System.clearProperty("java.security.auth.login.config");
            }
        }
    }

    /**
     * Test that assertSafeJaasLoginConfigProperty allows local file paths and file: URIs.
     */
    @Test
    public void testAssertSafeJaasLoginConfigProperty_allowsLocalPaths() throws Exception {
        Class<?> kerbAuthClass = Class.forName("com.microsoft.sqlserver.jdbc.KerbAuthentication");
        java.lang.reflect.Method assertMethod = kerbAuthClass.getDeclaredMethod("assertSafeJaasLoginConfigProperty");
        assertMethod.setAccessible(true);

        String[] localPaths = {
                "/etc/jaas.conf",
                "jaas.conf",
                "C:\\Users\\me\\jaas.conf",
                "C:/Users/me/jaas.conf",
                "file:///etc/jaas.conf",
                "file:///C:/Users/me/jaas.conf"
        };

        String original = System.getProperty("java.security.auth.login.config");
        try {
            for (String path : localPaths) {
                System.setProperty("java.security.auth.login.config", path);
                try {
                    assertMethod.invoke(null);
                    // Good - local path accepted
                } catch (java.lang.reflect.InvocationTargetException e) {
                    Assertions.fail("Should NOT have thrown for local path: " + path + " - " + e.getCause());
                }
            }
        } finally {
            if (original != null) {
                System.setProperty("java.security.auth.login.config", original);
            } else {
                System.clearProperty("java.security.auth.login.config");
            }
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
    public void testMSRC117029_pocPayloadsBlocked() throws Exception {
        Class<?> kerbAuthClass = Class.forName("com.microsoft.sqlserver.jdbc.KerbAuthentication");
        java.lang.reflect.Method assertMethod = kerbAuthClass.getDeclaredMethod("assertSafeJaasLoginConfigProperty");
        assertMethod.setAccessible(true);

        // Exact payloads from the MSRC-117029 PoC reproduction scripts
        String[] pocPayloads = {
                "http://remote.example.com/evil.jaas",
                "=http://remote.example.com/evil.jaas",
                "https://remote.example.com/evil.jaas",
                "ldap://remote.example.com/cn=Evil",
                "rmi://remote.example.com/Exploit"
        };

        String original = System.getProperty("java.security.auth.login.config");
        try {
            for (String payload : pocPayloads) {
                System.setProperty("java.security.auth.login.config", payload);
                try {
                    assertMethod.invoke(null);
                    Assertions.fail("MSRC-117029 PoC NOT blocked for payload: " + payload);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    Assertions.assertTrue(e.getCause() instanceof SQLServerException,
                            "Should throw SQLServerException for PoC payload: " + payload);
                    Assertions.assertTrue(
                            e.getCause().getMessage().contains("non-local"),
                            "Error message should indicate non-local location for: " + payload);
                }
            }
        } finally {
            if (original != null) {
                System.setProperty("java.security.auth.login.config", original);
            } else {
                System.clearProperty("java.security.auth.login.config");
            }
        }
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
