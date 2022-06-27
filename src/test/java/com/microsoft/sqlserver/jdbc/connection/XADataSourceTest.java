/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.connection;

import java.sql.Connection;

import javax.sql.XAConnection;

import com.microsoft.sqlserver.jdbc.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.reqExternalSetup)
public class XADataSourceTest extends AbstractTest {
    private static String connectionUrlSSL = connectionString;

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Tests XA connection with PKCS12 truststore that is password protected.
     * 
     * Only re-populate the truststore if need arises in the future. TestUtils.createTrustStore() can be used to create
     * the truststore.
     * 
     * @throws Exception
     */
    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.xAzureSQLDW)
    @Test
    public void testPKCS12() throws Exception {
        org.junit.Assume.assumeTrue(System.getProperty("os.name").startsWith("Windows"));

        SQLServerXADataSource ds = new SQLServerXADataSource();

        String trustStore = System.getProperty("pkcs12_truststore");
        String url = connectionUrlSSL + "trustStore=" + trustStore + ";";
        ds.setURL(url);
        ds.setTrustStorePassword(System.getProperty("pkcs12_truststore_password"));
        XAConnection connection = ds.getXAConnection();
        connection.close();
    }
}
