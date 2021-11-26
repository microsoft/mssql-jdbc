/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fips;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.microsoft.sqlserver.jdbc.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.StringUtils;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;;


/**
 * Test class for testing FIPS property settings.
 */
@RunWith(JUnitPlatform.class)
public class FipsTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        //Turn off default encrypt true
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"encrypt", "false");
        setConnection();
    }

    /**
     * Test after setting TrustServerCertificate as true.
     * 
     * @throws Exception when an error occurs
     */
    @Test
    public void fipsTrustServerCertificateTest() throws Exception {
        Properties props = buildConnectionProperties();
        props.setProperty(Constants.TRUST_SERVER_CERTIFICATE, Boolean.TRUE.toString());
        try (Connection con = PrepUtil.getConnection(connectionString, props)) {
            Assertions.fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLException e) {
            Assertions.assertTrue(e.getMessage().contains(TestResource.getResource("R_invalidFipsConfig")),
                    TestResource.getResource("R_invalidTrustCert"));
        }
    }

    /**
     * Test after passing encrypt as false.
     * 
     * @throws Exception when an error occurs
     */
    @Test
    public void fipsEncryptTest() throws Exception {
        Properties props = buildConnectionProperties();
        props.setProperty(Constants.ENCRYPT, Boolean.FALSE.toString());
        try (Connection con = PrepUtil.getConnection(connectionString, props)) {
            Assertions.fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLException e) {
            Assertions.assertTrue(e.getMessage().contains(TestResource.getResource("R_invalidFipsConfig")),
                    TestResource.getResource("R_invalidEncrypt"));
        }
    }

    /**
     * Test after removing fips, encrypt & trustStore it should work appropriately.
     * 
     * @throws Exception when an error occurs
     */
    @Test
    public void fipsPropertyTest() throws Exception {
        Properties props = buildConnectionProperties();
        props.remove(Constants.FIPS);
        props.remove(Constants.TRUST_STORE_TYPE);
        props.remove(Constants.ENCRYPT);
        props.remove(Constants.TRUST_SERVER_CERTIFICATE);

        try (Connection con = PrepUtil.getConnection(connectionString, props)) {
            Assertions.assertTrue(!StringUtils.isEmpty(con.getSchema()));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    /**
     * Tests after removing all FIPS related properties.
     * 
     * @throws Exception when an error occurs
     */
    @Test
    public void fipsDataSourcePropertyTest() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();
        setDataSourceProperties(ds);
        ds.setFIPS(false);
        ds.setEncrypt(false);
        ds.setTrustStoreType(Constants.JKS);
        try (Connection con = ds.getConnection()) {
            Assertions.assertTrue(!StringUtils.isEmpty(con.getSchema()));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    /**
     * Test after removing encrypt in FIPS Data Source.
     */
    @Test
    public void fipsDatSourceEncrypt() {
        SQLServerDataSource ds = new SQLServerDataSource();
        setDataSourceProperties(ds);
        ds.setEncrypt(false);

        try (Connection con = ds.getConnection()) {
            Assertions.fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLException e) {
            Assertions.assertTrue(e.getMessage().contains(TestResource.getResource("R_invalidFipsConfig")),
                    TestResource.getResource("R_invalidEncrypt"));
        }
    }

    /**
     * Test after setting TrustServerCertificate as true.
     * 
     * @throws Exception when an error occurs
     */
    @Test
    public void fipsDataSourceTrustServerCertificateTest() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();
        setDataSourceProperties(ds);
        ds.setTrustServerCertificate(true);

        try (Connection con = ds.getConnection()) {
            Assertions.fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLException e) {
            Assertions.assertTrue(e.getMessage().contains(TestResource.getResource("R_invalidFipsConfig")),
                    TestResource.getResource("R_invalidTrustCert"));
        }
    }

    /**
     * Setting appropriate data source properties including FIPS
     * 
     * @param ds
     */
    private void setDataSourceProperties(SQLServerDataSource ds) {
        ds = (SQLServerDataSource) updateDataSource(connectionString, ds);

        // Set all properties for FIPS
        ds.setFIPS(true);
        ds.setEncrypt(true);
        ds.setTrustServerCertificate(false);
        ds.setIntegratedSecurity(false);
        ds.setTrustStoreType(Constants.PKCS12);
    }

    /**
     * Build Connection properties for FIPS
     * 
     * @return
     */
    private Properties buildConnectionProperties() {
        Properties connectionProps = new Properties();

        connectionProps.setProperty(Constants.ENCRYPT, Boolean.TRUE.toString());
        connectionProps.setProperty(Constants.INTEGRATED_SECURITY, Boolean.FALSE.toString());

        // In case of false we need to pass keystore etc. which is not passing by default.
        connectionProps.setProperty(Constants.TRUST_SERVER_CERTIFICATE, Boolean.FALSE.toString());

        // For New Code
        connectionProps.setProperty(Constants.TRUST_STORE_TYPE, Constants.PKCS12);
        connectionProps.setProperty(Constants.FIPS, Boolean.TRUE.toString());

        return connectionProps;
    }
}
