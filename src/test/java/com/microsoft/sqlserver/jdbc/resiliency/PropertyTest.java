/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.resiliency;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@Tag(Constants.xSQLv11)
@Tag(Constants.xAzureSQLDW)
public class PropertyTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    private void testInvalidPropertyOverBrokenConnection(String prop, String val,
            String expectedErrMsg) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append(connectionString).append(";").append(prop).append("=").append(val).append(";");
        try (Connection c = ResiliencyUtils.getConnection(sb.toString())) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString, 0);
                s.executeQuery("SELECT 1");
                fail(TestResource.getResource("R_expectedExceptionNotThrown") + prop + "=" + val);
            }
        } catch (SQLException e) {
            assertTrue(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage(),
                    e.getMessage().matches(expectedErrMsg));
        }
    }

    @Test
    public void testRetryCount() throws SQLException {
        // fail immediately without retrying
        testInvalidPropertyOverBrokenConnection("connectRetryCount", "0", ".*");
        // Out of range, < 0
        testInvalidPropertyOverBrokenConnection("connectRetryCount",
                String.valueOf(ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, 0)),
                TestUtils.formatErrorMsg("R_invalidConnectRetryCount"));
        // Out of range, > 255
        testInvalidPropertyOverBrokenConnection("connectRetryCount",
                String.valueOf(ThreadLocalRandom.current().nextInt(256, Integer.MAX_VALUE)),
                TestUtils.formatErrorMsg("R_invalidConnectRetryCount"));
        // non-Integer types: boolean, float, double, string
        testInvalidPropertyOverBrokenConnection("connectRetryCount",
                String.valueOf(ThreadLocalRandom.current().nextBoolean()),
                TestUtils.formatErrorMsg("R_invalidConnectRetryCount"));
        testInvalidPropertyOverBrokenConnection("connectRetryCount",
                String.valueOf(ThreadLocalRandom.current().nextFloat()),
                TestUtils.formatErrorMsg("R_invalidConnectRetryCount"));
        testInvalidPropertyOverBrokenConnection("connectRetryCount",
                String.valueOf(ThreadLocalRandom.current().nextDouble()),
                TestUtils.formatErrorMsg("R_invalidConnectRetryCount"));
        testInvalidPropertyOverBrokenConnection("connectRetryCount",
                ResiliencyUtils.getRandomString(ResiliencyUtils.alpha, 15),
                TestUtils.formatErrorMsg("R_invalidConnectRetryCount"));
    }

    @Test
    public void testRetryInterval() throws SQLException {
        // Out of range, < 1
        testInvalidPropertyOverBrokenConnection("connectRetryInterval",
                String.valueOf(ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, 1)),
                TestUtils.formatErrorMsg("R_invalidConnectRetryInterval"));
        // Out of range, > 60
        testInvalidPropertyOverBrokenConnection("connectRetryInterval",
                String.valueOf(ThreadLocalRandom.current().nextInt(61, Integer.MAX_VALUE)),
                TestUtils.formatErrorMsg("R_invalidConnectRetryInterval"));
        // non-Integer types: boolean, float, double, string
        testInvalidPropertyOverBrokenConnection("connectRetryInterval",
                String.valueOf(ThreadLocalRandom.current().nextBoolean()),
                TestUtils.formatErrorMsg("R_invalidConnectRetryInterval"));
        testInvalidPropertyOverBrokenConnection("connectRetryInterval",
                String.valueOf(ThreadLocalRandom.current().nextFloat()),
                TestUtils.formatErrorMsg("R_invalidConnectRetryInterval"));
        testInvalidPropertyOverBrokenConnection("connectRetryInterval",
                String.valueOf(ThreadLocalRandom.current().nextDouble()),
                TestUtils.formatErrorMsg("R_invalidConnectRetryInterval"));
        testInvalidPropertyOverBrokenConnection("connectRetryInterval",
                ResiliencyUtils.getRandomString(ResiliencyUtils.alpha, 15),
                TestUtils.formatErrorMsg("R_invalidConnectRetryInterval"));
    }
}
