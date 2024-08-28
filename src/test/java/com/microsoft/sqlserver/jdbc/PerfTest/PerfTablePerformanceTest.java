package com.microsoft.sqlserver.jdbc.PerfTest;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

//import com.google.common.base.Stopwatch;

public class PerfTablePerformanceTest implements PerformanceTest
{
    private final int fetchSize;
    public PerfTablePerformanceTest(int fetchSize)
    {
        this.fetchSize = fetchSize;
    }

    public String getName()
    {
        return "PERF_TABLE(" + fetchSize + ")";
    }

    @Override
    public String getDescription() {
        return "SELECT * FROM PERF_TABLE(" + (fetchSize == 0 ? "default" : String.valueOf(fetchSize)) + ")";
    }

    @Override
    public PerformanceResult run(Connection conn, int resultSetType) throws SQLException
    {
        //Stopwatch stopwatch = Stopwatch.createStarted();
        int numberOfRecords = 0;
        long totalBytes = 0L;
        int nullColumnCount = 0;
        int columns = 0;
        int rsFetchSize = 0;
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
                    int id = rs.getInt(columnIndex++);
                    totalBytes += 4;
                    for (int repeat = 0; repeat < PreparePerfTables.COLUMN_REPEAT_COUNT; repeat++) {
                        String indicatorValue = rs.getString(columnIndex++);
                        if (indicatorValue == null) {
                            nullColumnCount++;
                        } else {
                            totalBytes += 1;
                        }
                        String stringValue = rs.getString(columnIndex++);
                        if (stringValue == null) {
                            nullColumnCount++;
                        } else {
                            totalBytes += stringValue.length();
                        }
                        long longValue = rs.getLong(columnIndex++);
                        if (rs.wasNull()) {
                            longValue = Long.MIN_VALUE;
                            nullColumnCount++;
                        } else {
                            totalBytes += 8;
                        }
                        double doubleValue = rs.getDouble(columnIndex++);
                        if (rs.wasNull()) {
                            doubleValue = Double.NaN;
                            nullColumnCount++;
                        } else {
                            totalBytes += 8;
                        }
                        BigDecimal decimalValue = rs.getBigDecimal(columnIndex++);
                        if (decimalValue == null) {
                            nullColumnCount++;
                        } else {
                            totalBytes += 12;
                        }
                        Timestamp timestampValue = rs.getTimestamp(columnIndex++);
                        if (timestampValue == null) {
                            nullColumnCount++;
                        } else {
                            totalBytes += 12;
                        }
                    }
                }
            }
        }
        final long elapsed = System.currentTimeMillis();
        System.out.println("Retrieved " + numberOfRecords + " rows in " + elapsed + " ms.");
        return new PerformanceResult(elapsed, numberOfRecords, totalBytes, nullColumnCount, columns, rsFetchSize);
    }
}
