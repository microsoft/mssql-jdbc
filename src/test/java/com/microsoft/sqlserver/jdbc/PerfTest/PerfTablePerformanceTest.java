package com.microsoft.sqlserver.jdbc.PerfTest;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

public class PerfTablePerformanceTest implements PerformanceTest {
    private final int fetchSize;
    public PerfTablePerformanceTest(int fetchSize)
    {
        this.fetchSize = fetchSize;
    }

    public String getName()
    {
        return "PERF_TABLE(" + fetchSize + ")";
    }

    long intTimes = 0;
    long string1Times = 0;
    long string2Times = 0;
    long longTimes = 0;
    long doubleTimes = 0;
    long decimalTimes = 0;
    long timestampTimes = 0;
    long totalCount = 0;

    @Override
    public String getDescription() {
        return "SELECT * FROM PERF_TABLE(" + (fetchSize == 0 ? "default" : String.valueOf(fetchSize)) + ")";
    }

    @Override
    public PerformanceResult run(Connection conn, int resultSetType) throws SQLException {
        long stopwatch = System.currentTimeMillis();

        int numberOfRecords = 0;
        long totalBytes = 0L;
        int nullColumnCount = 0;
        int columns;
        int rsFetchSize;
        try (Statement stmt = conn.createStatement(resultSetType, ResultSet.CONCUR_READ_ONLY)) {
            if (fetchSize != 0) {
                int stmtFetchSize = stmt.getFetchSize();
                if (fetchSize != stmtFetchSize) {
                    stmt.setFetchSize(fetchSize);
                    System.out.println("Changed stmt.fetchSize from " + stmtFetchSize + " to " + fetchSize);
                }
            }
            String sql = "SELECT T.* FROM [dbo].[PERF_TABLE] T CROSS JOIN [dbo].[PERF_MULTIPLIER] M WHERE M.ID <= 1000";
            try (ResultSet rs = stmt.executeQuery(sql)) {
                rsFetchSize = rs.getFetchSize();
                if (fetchSize != 0 && fetchSize != rsFetchSize) {
                    rs.setFetchSize(fetchSize);
                    System.out.println("Changed rs.fetchSize from " + rsFetchSize + " to " + fetchSize);
                    rsFetchSize = fetchSize;
                }
                columns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    numberOfRecords++;
                    int columnIndex = 1;
                    long intTimer = System.currentTimeMillis();
                    int id = rs.getInt(columnIndex++);
                    intTimes += System.currentTimeMillis() - intTimer;
                    totalCount++;

                    totalBytes += 4;
                    for (int repeat = 0; repeat < PreparePerfTables.COLUMN_REPEAT_COUNT; repeat++) {
                        long string1Timer = System.currentTimeMillis();
                        String indicatorValue = rs.getString(columnIndex++);
                        string1Times += System.currentTimeMillis() - string1Timer;

                        long string2Timer = System.currentTimeMillis();
                        String stringValue = rs.getString(columnIndex++);
                        string2Times += System.currentTimeMillis() - string2Timer;

                        long longTimer = System.currentTimeMillis();
                        long longValue = rs.getLong(columnIndex++); /**/
                        longTimes += System.currentTimeMillis() - longTimer;

                        long doubleTimer = System.currentTimeMillis();
                        double doubleValue = rs.getDouble(columnIndex++); /**/
                        doubleTimes += System.currentTimeMillis() - doubleTimer;

                        long decimalTimer = System.currentTimeMillis();
                        BigDecimal decimalValue = rs.getBigDecimal(columnIndex++); /**/
                        decimalTimes += System.currentTimeMillis() - decimalTimer;

                        long timestampTimer = System.currentTimeMillis();
                        Timestamp timestampValue = rs.getTimestamp(columnIndex++);
                        timestampTimes += System.currentTimeMillis() - timestampTimer;
                    }
                }
            }
        }
        final long elapsed = System.currentTimeMillis() - stopwatch;
        //System.out.println("Retrieved " + numberOfRecords + " rows in " + elapsed + " ms.");
        System.out.println("int times: " + intTimes + " ms");
        System.out.println("string1 times: " + string1Times + " ms");
        System.out.println("string2 times: " + string2Times + " ms");
        System.out.println("long times: " + longTimes + " ms");
        System.out.println("double times: " + doubleTimes + " ms");
        System.out.println("big decimal times: " + decimalTimes + " ms");
        System.out.println("timestamp times: " + timestampTimes + " ms");

        return new PerformanceResult(elapsed, numberOfRecords, totalBytes, nullColumnCount, columns, rsFetchSize);
    }
}
