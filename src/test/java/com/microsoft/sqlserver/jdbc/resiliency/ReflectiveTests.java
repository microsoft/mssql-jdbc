/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.resiliency;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@Tag(Constants.xSQLv11)
@Tag(Constants.xAzureSQLDW)
public class ReflectiveTests extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    private void timeoutVariations(Map<String, String> props, long expectedDuration,
            Optional<String> expectedErrMsg) throws SQLException {
        long startTime = 0;
        String cs = ResiliencyUtils.setConnectionProps(connectionString.concat(";"), props);
        try (Connection c = ResiliencyUtils.getConnection(cs)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString, 0);
                ResiliencyUtils.blockConnection(c);
                startTime = System.currentTimeMillis();
                s.executeQuery("SELECT 1");
                fail("Successfully executed query on a blocked connection.");
            } catch (SQLException e) {
                double elapsedTime = System.currentTimeMillis() - startTime;

                // Timeout should occur after query timeout and not login timeout
                assertTrue("Exception: " + e.getMessage() + ": Query did not timeout in " + expectedDuration
                        + "ms, elapsed time(ms): " + elapsedTime, elapsedTime < expectedDuration);
                if (expectedErrMsg.isPresent()) {
                    assertTrue(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage(),
                            e.getMessage().matches(TestUtils.formatErrorMsg(expectedErrMsg.get())));
                }
            }
        }
    }

    /*
     * Command with default ReconnectRetryDelay is executed over a broken connection Expected: Client waits exactly 10
     * seconds between attempts Expected to timeout in 20 seconds, (retryDelay * (retryCount-1)) +
     * (loginTimeout*retryCount)
     */
    @Test
    public void testDefaultTimeout() throws SQLException {
        Map<String, String> m = new HashMap<>();
        m.put("connectRetryCount", "2");
        m.put("loginTimeout", "5");
        timeoutVariations(m, 25000, Optional.empty());
    }

    /*
     * Default retry count is 1. Expect timeout to be just above login timeout.
     */
    @Test
    public void testDefaultRetry() throws SQLException {
        Map<String, String> m = new HashMap<>();
        m.put("loginTimeout", "5");

        // ensure count is not set to something else as this test assumes exactly just 1 retry
        m.put("connectRetryCount", "1");
        timeoutVariations(m, 6000, Optional.empty());
    }

    /*
     * Command with ReconnectRetryDelay > QueryTimeout is executed over a broken connection Expected: Client reports
     * query timeout immediately without waiting for ReconenctRetryDelay
     */
    @Test
    public void testReconnectDelayQueryTimeout() throws SQLException {
        Map<String, String> m = new HashMap<>();
        m.put("connectRetryCount", "3");
        m.put("connectRetryInterval", "30");
        m.put("queryTimeout", "5");
        timeoutVariations(m, 10000, Optional.empty());
    }

    /*
     * Command with infinite ConnectionTimeout and ReconnectRetryCount == 1 is executed over a broken connection
     * Expected: Client times out by QueryTimeout
     */
    @Test
    public void testQueryTimeout() throws SQLException {
        Map<String, String> m = new HashMap<>();
        m.put("queryTimeout", "10");
        m.put("loginTimeout", "65535");
        m.put("connectRetryCount", "1");
        // The timeout happens in < 10s about 55% of the time and < 12s ~95% of the time in pipelines.
        // Using 14s to ensure we don't needlessly fail for the last ~5%.
        timeoutVariations(m, 14000, Optional.empty());
    }

    /*
     * Command with QueryTimeout > (ReconnectRetryCount * (ReconnectRetryDelay + ConnectionTimeout)) is executed over a
     * broken connection Expected: Client fails after ReconnectRetryCount attempts
     */
    @Test
    public void testValidRetryWindow() throws SQLException {
        Map<String, String> m = new HashMap<>();
        m.put("queryTimeout", "-1");
        m.put("loginTimeout", "5");
        m.put("connectRetryCount", "2");
        m.put("connectRetryInterval", "10");
        timeoutVariations(m, 25000, Optional.of("R_crClientAllRecoveryAttemptsFailed"));
    }

    @Test
    public void testRequestRecovery() throws SQLException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, SecurityException, InvocationTargetException {
        Map<String, String> m = new HashMap<>();
        m.put("connectRetryCount", "1");
        String cs = ResiliencyUtils.setConnectionProps(connectionString.concat(";"), m);

        try (Connection c = DriverManager.getConnection(cs)) {
            Field fields[] = c.getClass().getSuperclass().getDeclaredFields();
            for (Field f : fields) {
                if (f.getName() == "sessionRecovery") {
                    f.setAccessible(true);
                    Object sessionRecoveryFeature = f.get(c);
                    Method method = sessionRecoveryFeature.getClass()
                            .getDeclaredMethod("isConnectionRecoveryNegotiated");
                    method.setAccessible(true);
                    boolean b = (boolean) method.invoke(sessionRecoveryFeature);
                    assertTrue("Session Recovery not negotiated when requested", b);
                }
            }
        }
    }

    @Test
    public void testNoRecovery() throws SQLException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, SecurityException, InvocationTargetException {
        Map<String, String> m = new HashMap<>();
        m.put("connectRetryCount", "0");
        String cs = ResiliencyUtils.setConnectionProps(connectionString.concat(";"), m);

        try (Connection c = DriverManager.getConnection(cs)) {
            Field fields[] = c.getClass().getSuperclass().getDeclaredFields();
            for (Field f : fields) {
                if (f.getName() == "sessionRecovery") {
                    f.setAccessible(true);
                    Object sessionRecoveryFeature = f.get(c);
                    Method method = sessionRecoveryFeature.getClass()
                            .getDeclaredMethod("isConnectionRecoveryNegotiated");
                    method.setAccessible(true);
                    assertTrue("Session Recovery received when not negotiated",
                            !(boolean) method.invoke(sessionRecoveryFeature));
                }
            }
        }
    }
}
