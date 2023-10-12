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

@RunWith(JUnitPlatform.class)
public class KerberosTest extends AbstractTest {

    private static String kerberosAuth = "KERBEROS";

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Tag(Constants.Kerberos)
    @Test
    public void testUseDefaultJaasConfigConnectionStringPropertyTrue() throws Exception {
        String connectionStringUseDefaultJaasConfig = connectionStringKerberos + ";useDefaultJaasConfig=true;";

        // Initial connection should succeed with default JAAS config
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionStringUseDefaultJaasConfig)) {
            ResultSet rs = conn.createStatement().executeQuery("select auth_scheme from sys.dm_exec_connections where session_id=@@spid");
            rs.next();
            Assertions.assertEquals(kerberosAuth, rs.getString(1));
        }

        // Attempt to overwrite JAAS config. Since useDefaultJaasConfig=true, this should have no effect
        // and subsequent connections should succeed.
        overwriteJaasConfig();

        // New connection should successfully connect and continue to use the default JAAS config.
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionStringUseDefaultJaasConfig)) {
            ResultSet rs = conn.createStatement().executeQuery("select auth_scheme from sys.dm_exec_connections where session_id=@@spid");
            rs.next();
            Assertions.assertEquals(kerberosAuth, rs.getString(1));
        }
    }

    @Tag(Constants.Kerberos)
    @Test
    public void testUseDefaultJaasConfigConnectionStringPropertyFalse() throws Exception {

        // useDefaultJaasConfig=false by default
        // Initial connection should succeed with default JAAS config
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionStringKerberos)) {
            ResultSet rs = conn.createStatement().executeQuery("select auth_scheme from sys.dm_exec_connections where session_id=@@spid");
            rs.next();
            Assertions.assertEquals(kerberosAuth, rs.getString(1));
        }

        // Overwrite JAAS config. Since useDefaultJaasConfig=false, overwriting should succeed and have an effect.
        // Subsequent connections will fail.
        overwriteJaasConfig();

        // New connection should fail as it is attempting to connect using an overwritten JAAS config.
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionStringKerberos)) {
            Assertions.fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            Assertions.assertTrue(e.getMessage()
                    .contains(TestResource.getResource("R_noLoginModulesConfiguredForJdbcDriver")));
        }
    }

    /**
     * Overwrites the default JAAS config. Call before making a connection.
     */
    private static void overwriteJaasConfig() {
        AppConfigurationEntry kafkaClientConfigurationEntry = new AppConfigurationEntry(
                "com.sun.security.auth.module.Krb5LoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                new HashMap<>());
        Map<String, AppConfigurationEntry[]> configurationEntries = new HashMap<>();
        configurationEntries.put("KAFKA_CLIENT_CONTEXT_NAME",
                new AppConfigurationEntry[] { kafkaClientConfigurationEntry });
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
