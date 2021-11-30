/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.ssl.trustmanager;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import com.microsoft.sqlserver.jdbc.TestResource;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


@RunWith(JUnitPlatform.class)
public class CustomTrustManagerTest extends AbstractTest {

    /**
     * Connect with a permissive Trust Manager that always accepts the X509Certificate chain offered to it.
     * 
     * @throws Exception
     */
    @Test
    public void testWithPermissiveX509TrustManager() throws Exception {
        String url = connectionString + ";trustManagerClass=" + PermissiveTrustManager.class.getName()
                + ";encrypt=true;";
        try (Connection con = PrepUtil.getConnection(url)) {
            assertTrue(con != null);
        }
    }

    /**
     * Connect with a Trust Manager that requires trustManagerConstructorArg.
     * 
     * @throws Exception
     */
    @Test
    public void testWithTrustManagerConstructorArg() throws Exception {
        String url = connectionString + ";trustManagerClass=" + TrustManagerWithConstructorArg.class.getName()
                + ";trustManagerConstructorArg=dummyString;" + ";encrypt=true;";
        try (Connection con = PrepUtil.getConnection(url)) {
            assertTrue(con != null);
        }
    }

    /**
     * Test with a custom Trust Manager that does not implement X509TrustManager.
     * 
     * @throws Exception
     */
    @Test
    public void testWithInvalidTrustManager() throws Exception {
        String url = connectionString + ";trustManagerClass=" + InvalidTrustManager.class.getName();
        try (Connection con = PrepUtil.getConnection(url)) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(
                    "The class specified by the trustManagerClass property must be assignable to javax.net.ssl.TrustManager."));
        }
    }
}
