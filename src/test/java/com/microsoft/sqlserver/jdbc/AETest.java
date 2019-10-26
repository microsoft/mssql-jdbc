package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.LogManager;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * A class for testing basic NTLMv2 functionality.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xSQLv12)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.reqExternalSetup)
public class AETest extends AbstractTest {

    private static SQLServerDataSource dsLocal = null;
    private static SQLServerDataSource dsXA = null;
    private static SQLServerDataSource dsPool = null;

    private static String connectionStringAE;

    /**
     * Setup environment for test.
     * 
     * @throws Exception
     *         when an error occurs
     */
    @BeforeAll
    public static void setUp() throws Exception {
        connectionStringAE = TestUtils.addOrOverrideProperty(connectionString, "columnEncryptionSetting",
                ColumnEncryptionSetting.Enabled.toString());

        String enclaveAttestationUrl = TestUtils.getConfiguredProperty("enclaveAttestationUrl");
        connectionStringAE = TestUtils.addOrOverrideProperty(connectionStringAE, "enclaveAttestationUrl",
                (null != enclaveAttestationUrl) ? enclaveAttestationUrl : "http://blah");

        String enclaveAttestationProtocol = TestUtils.getConfiguredProperty("enclaveAttestationProtocol");
        connectionStringAE = TestUtils.addOrOverrideProperty(connectionStringAE, "enclaveAttestationProtocol",
                (null != enclaveAttestationProtocol) ? enclaveAttestationProtocol : AttestationProtocol.HGS.toString());

        boolean isAEv2 = false;
        try (SQLServerConnection con = PrepUtil.getConnection(connectionStringAE)) {
            isAEv2 = TestUtils.isAEv2(con);
        } catch (SQLException e) {
            isAEv2 = false;
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
        org.junit.Assume.assumeTrue(isAEv2);

        // reset logging to avoid severe logs due to negative testing
        LogManager.getLogManager().reset();

        dsLocal = new SQLServerDataSource();
        AbstractTest.updateDataSource(connectionStringAE, dsLocal);

        dsXA = new SQLServerXADataSource();
        AbstractTest.updateDataSource(connectionStringAE, dsXA);

        dsPool = new SQLServerConnectionPoolDataSource();
        AbstractTest.updateDataSource(connectionStringAE, dsPool);

    }

    /**
     * Tests basic connection.
     * 
     * @throws SQLException
     *         when an error occurs
     */
    @Test
    public void testBasicConnection() throws SQLException {
        try (Connection con1 = dsLocal.getConnection(); Connection con2 = dsXA.getConnection();
                Connection con3 = dsPool.getConnection();
                Connection con4 = PrepUtil.getConnection(connectionStringAE)) {
            if (TestUtils.isAEv2(con1)) {
                verifyEnclaveEnabled(con1);
            }
            if (TestUtils.isAEv2(con2)) {
                verifyEnclaveEnabled(con2);
            }
            if (TestUtils.isAEv2(con3)) {
                verifyEnclaveEnabled(con3);
            }
            if (TestUtils.isAEv2(con4)) {
                verifyEnclaveEnabled(con4);
            }
        }
    }

    /**
     * Tests invalid connection property combinations.
     */
    @Test
    public void testInvalidProperties() {
        // enclaveAttestationUrl and enclaveAttestationProtocol without "columnEncryptionSetting"
        testInvalidProperties(TestUtils.addOrOverrideProperty(connectionStringAE, "columnEncryptionSetting",
                ColumnEncryptionSetting.Disabled.toString()), "R_enclaveAEdisabled");

        // enclaveAttestationUrl without enclaveAttestationProtocol
        testInvalidProperties(TestUtils.removeProperty(connectionStringAE, "enclaveAttestationProtocol"),
                "R_enclavePropertiesError");

        // enclaveAttestationProtocol without enclaveAttestationUrl
        testInvalidProperties(TestUtils.addOrOverrideProperty(connectionStringAE, "enclaveAttestationUrl", ""),
                "R_enclavePropertiesError");

        // bad enclaveAttestationProtocol
        testInvalidProperties(TestUtils.addOrOverrideProperty(connectionStringAE, "enclaveAttestationProtocol", ""),
                "R_enclaveInvalidAttestationProtocol");

    }

    /*
     * Test getEnclavePackage with null enclaveSession
     */
    @Test
    public void testGetEnclavePackage() {
        SQLServerVSMEnclaveProvider provider = new SQLServerVSMEnclaveProvider();
        try {
            provider.getEnclavePackage(null, null);
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException"));
        }

    }

    /*
     * Test invalidEnclaveSession
     */
    @Test
    public void testInvalidEnclaveSession() {
        SQLServerVSMEnclaveProvider provider = new SQLServerVSMEnclaveProvider();
        provider.invalidateEnclaveSession();
        if (null != provider.getEnclaveSession()) {
            fail(TestResource.getResource("R_invalidEnclaveSessionFailed"));
        }
    }

    /*
     * Test VSM createSessionSecret with bad server response
     */
    @Test
    public void testCreateSessionSecret() throws SQLServerException {
        VSMAttestationParameters param = new VSMAttestationParameters();

        try {
            param.createSessionSecret(null);
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_MalformedECDHPublicKey")));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }

        try {
            byte[] serverResponse = new byte[104]; // ENCLAVE_LENGTH is private
            param.createSessionSecret(serverResponse);
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_MalformedECDHHeader")));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /*
     * Test invalid properties
     */
    private void testInvalidProperties(String connStr, String resourceKeyword) {
        try (Connection con = PrepUtil.getConnection(connStr)) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg(resourceKeyword)), e.getMessage());
        }
    }

    /*
     * Verify if Enclave is enabled
     */
    private void verifyEnclaveEnabled(Connection con) throws SQLException {
        try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery(
                "SELECT [name], [value], [value_in_use] FROM sys.configurations WHERE [name] = 'column encryption enclave type';")) {
            while (rs.next()) {
                assertEquals("1", rs.getString(2));
            }
        }
    }
}
