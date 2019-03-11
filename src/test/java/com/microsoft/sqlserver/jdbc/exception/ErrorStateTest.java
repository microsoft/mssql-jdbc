package com.microsoft.sqlserver.jdbc.exception;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


@RunWith(JUnitPlatform.class)
public class ErrorStateTest extends AbstractTest {

    @Test
    public void testSQLStateNegative() throws Exception {
        int state = -1; // Negative error raised converts to positive SQL State (1)
        try (Connection con = PrepUtil.getConnection(connectionString); Statement stmt = con.createStatement()) {
            stmt.execute("RAISERROR (13002, -1, " + state + ", N'Testing error');");
        } catch (SQLException e) {
            assert (e.getSQLState().length() == 5);
            assert (e.getSQLState().equalsIgnoreCase("S0001"));
        }
    }

    @Test
    public void testSQLStateLength1() throws Exception {
        try (Connection con = PrepUtil.getConnection(connectionString); Statement stmt = con.createStatement()) {
            stmt.execute("SELECT 1/0;");
        } catch (SQLException e) {
            assert (e.getSQLState().length() == 5);
            assert (e.getSQLState().equalsIgnoreCase("S0001"));
        }
    }

    @Test
    public void testSQLStateLength2() throws Exception {
        int state = 31;
        try (Connection con = PrepUtil.getConnection(connectionString); Statement stmt = con.createStatement()) {
            stmt.execute("RAISERROR (13002, -1, " + state + ", N'Testing error');");
        } catch (SQLException e) {
            assert (e.getSQLState().length() == 5);
            assert (e.getSQLState().equalsIgnoreCase("S00" + state));
        }
    }

    @Test
    public void testSQLStateLength3() throws Exception {
        int state = 255; // Max Value of SQL State
        try (Connection con = PrepUtil.getConnection(connectionString); Statement stmt = con.createStatement()) {
            stmt.execute("RAISERROR (13003, -1, " + state + ", N'Testing error');");
        } catch (SQLException e) {
            assert (e.getSQLState().length() == 5);
            assert (e.getSQLState().equalsIgnoreCase("S0" + state));
        }
    }
}
