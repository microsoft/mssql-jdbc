package com.microsoft.sqlserver.jdbc.PerfTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;


public class TestClass {

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        String url = "jdbc:sqlserver://localhost:1433;DatabaseName=TestDb;encrypt=false;sendStringParametersAsUnicode=false;enablePrepareOnFirstPreparedStatementCall=true;userName=employee;password=Moonshine4me";
        String jtDS = "jdbc:jtds:sqlserver://localhost:1433/TestDb;user=employee;password=Moonshine4me";

        try (Connection conn = DriverManager.getConnection(jtDS); Statement stmt = conn.createStatement()) {
            String sql = "SELECT T.* FROM [dbo].[PERF_TABLE] T CROSS JOIN [dbo].[PERF_MULTIPLIER] M WHERE M.ID <= 1000";

            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                int columnIndex = 1;
                rs.getInt(columnIndex++);
                long timerNow = System.currentTimeMillis();
                rs.getString(columnIndex++);
                System.out.println("Time for getString() jTDS: " + (System.currentTimeMillis() - timerNow) + " ms");
                timerNow = System.currentTimeMillis();
                rs.getString(columnIndex++);
                System.out.println("Time for getString() jTDS: " + (System.currentTimeMillis() - timerNow) + " ms");
                timerNow = System.currentTimeMillis();
                rs.getLong(columnIndex++);
                System.out.println("Time for getLong() jTDS: " + (System.currentTimeMillis() - timerNow) + " ms");
                timerNow = System.currentTimeMillis();
                rs.getDouble(columnIndex++);
                System.out.println("Time for getDouble() jTDS: " + (System.currentTimeMillis() - timerNow) + " ms");
                timerNow = System.currentTimeMillis();
                rs.getBigDecimal(columnIndex++);
                System.out.println("Time for getBigDecimal() jTDS: " + (System.currentTimeMillis() - timerNow) + " ms");
                timerNow = System.currentTimeMillis();
                rs.getTimestamp(columnIndex);
                System.out.println("Time for getTimestamp() jTDS: " + (System.currentTimeMillis() - timerNow) + " ms");
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println();

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(url);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            String sql = "SELECT T.* FROM [dbo].[PERF_TABLE] T CROSS JOIN [dbo].[PERF_MULTIPLIER] M WHERE M.ID <= 1000";

            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                int columnIndex = 1;
                rs.getInt(columnIndex++);
                long timerNow = System.currentTimeMillis();
                rs.getString(columnIndex++);
                System.out.println("Time for getString() JDBC: " + (System.currentTimeMillis() - timerNow) + " ms");
                timerNow = System.currentTimeMillis();
                rs.getString(columnIndex++);
                System.out.println("Time for getString() JDBC: " + (System.currentTimeMillis() - timerNow) + " ms");
                timerNow = System.currentTimeMillis();
                rs.getLong(columnIndex++);
                System.out.println("Time for getLong() JDBC: " + (System.currentTimeMillis() - timerNow) + " ms");
                timerNow = System.currentTimeMillis();
                rs.getDouble(columnIndex++);
                System.out.println("Time for getDouble() JDBC: " + (System.currentTimeMillis() - timerNow) + " ms");
                timerNow = System.currentTimeMillis();
                rs.getBigDecimal(columnIndex++);
                System.out.println("Time for getBigDecimal() JDBC: " + (System.currentTimeMillis() - timerNow) + " ms");
                timerNow = System.currentTimeMillis();
                rs.getTimestamp(columnIndex);
                System.out.println("Time for getTimestamp() JDBC: " + (System.currentTimeMillis() - timerNow) + " ms");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
