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
