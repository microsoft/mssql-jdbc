package com.microsoft.sqlserver.jdbc.PerfTest;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import java.sql.*;

public class TestClass2 {
    static long jtdsString1 = 0;
    static long jtdsString2 = 0;
    static long jtdsLong = 0;
    static long jtdsDouble = 0;
    static long jtdsBigDecimal = 0;
    static long jtdsTimestamp = 0;
    static long jdbcString1 = 0;
    static long jdbcString2 = 0;
    static long jdbcLong = 0;
    static long jdbcDouble = 0;
    static long jdbcBigDecimal = 0;
    static long jdbcTimestamp = 0;
    static int runCount = 10;

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < runCount; i++) {
            run();
        }

        System.out.println("Time for getString() jTDS: " + (jtdsString1 / runCount) + " ms");
        System.out.println("Time for getString2() jTDS: " + (jtdsString2 / runCount) + " ms");
        System.out.println("Time for getLong() jTDS: " + (jtdsLong / runCount) + " ms");
        System.out.println("Time for getDouble() jTDS: " + (jtdsDouble / runCount) + " ms");
        System.out.println("Time for getBigDecimal() jTDS: " + (jtdsBigDecimal / runCount) + " ms");
        System.out.println("Time for getTimestamp() jTDS: " + (jtdsTimestamp / runCount) + " ms");

        System.out.println();

        System.out.println("Time for getString() JDBC: " + (jdbcString1 / runCount) + " ms");
        System.out.println("Time for getString2() JDBC: " + (jdbcString2 / runCount) + " ms");
        System.out.println("Time for getLong() JDBC: " + (jdbcLong / runCount) + " ms");
        System.out.println("Time for getDouble() JDBC: " + (jdbcDouble / runCount) + " ms");
        System.out.println("Time for getBigDecimal() JDBC: " + (jdbcBigDecimal / runCount) + " ms");
        System.out.println("Time for getTimestamp() JDBC: " + (jdbcTimestamp / runCount) + " ms");
    }

    public static void run() throws Exception {
        String url = "jdbc:sqlserver://localhost:1433;DatabaseName=TestDb;encrypt=false;sendStringParametersAsUnicode=false;enablePrepareOnFirstPreparedStatementCall=true;userName=sa;password=TestPassword123";
        String jtDS = "jdbc:jtds:sqlserver://localhost:1433/TestDb;user=sa;password=TestPassword123";

        try (Connection conn = DriverManager.getConnection(jtDS); Statement stmt = conn.createStatement()) {
            String sql = "SELECT T.* FROM [dbo].[PERF_TABLE] T CROSS JOIN [dbo].[PERF_MULTIPLIER] M WHERE M.ID <= 1000";

            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                int columnIndex = 1;
                rs.getInt(columnIndex++);
                rs.getString(columnIndex++);
                long timerNow = System.currentTimeMillis();

                jtdsString1 += System.currentTimeMillis() - timerNow;
                timerNow = System.currentTimeMillis();
                rs.getString(columnIndex++);

                jtdsString2 += System.currentTimeMillis() - timerNow;
                timerNow = System.currentTimeMillis();
                rs.getLong(columnIndex++);

                jtdsLong += System.currentTimeMillis() - timerNow;
                timerNow = System.currentTimeMillis();
                rs.getDouble(columnIndex++);

                jtdsDouble += System.currentTimeMillis() - timerNow;
                timerNow = System.currentTimeMillis();
                rs.getBigDecimal(columnIndex++);

                jtdsBigDecimal += System.currentTimeMillis() - timerNow;
                timerNow = System.currentTimeMillis();
                rs.getTimestamp(columnIndex);

                jtdsTimestamp += System.currentTimeMillis() - timerNow;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(url);

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
            String sql = "SELECT T.* FROM [dbo].[PERF_TABLE] T CROSS JOIN [dbo].[PERF_MULTIPLIER] M WHERE M.ID <= 1000";

            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                int columnIndex = 1;
                rs.getInt(columnIndex++);
                rs.getString(columnIndex++);
                long timerNow = System.currentTimeMillis();

                jdbcString1 += System.currentTimeMillis() - timerNow;
                timerNow = System.currentTimeMillis();
                rs.getString(columnIndex++);

                jdbcString2 += System.currentTimeMillis() - timerNow;
                timerNow = System.currentTimeMillis();
                rs.getLong(columnIndex++);

                jdbcLong += System.currentTimeMillis() - timerNow;
                timerNow = System.currentTimeMillis();
                rs.getDouble(columnIndex++);

                jdbcDouble += System.currentTimeMillis() - timerNow;
                timerNow = System.currentTimeMillis();
                rs.getBigDecimal(columnIndex++);

                jdbcBigDecimal += System.currentTimeMillis() - timerNow;
                timerNow = System.currentTimeMillis();
                rs.getTimestamp(columnIndex);

                jdbcTimestamp += System.currentTimeMillis() - timerNow;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
