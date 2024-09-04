package com.microsoft.sqlserver.jdbc.PerfTest;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Test jdbc driver's performance
 */
public interface PerformanceTest {

    /**
     * @return -- the name of the test
     */
    String getName();

    /**
     * @return -- the description of the test
     */
    String getDescription();

    /**
     * run the test
     * @param conn connection
     * @param resultSetType result set type
     * @return -- the first captures execution time,  while the second is result count
     * @throws SQLException if an error occurs
     */
    PerformanceResult run(Connection conn, int resultSetType) throws SQLException;

    record PerformanceResult(long elapsedMilliseconds, int numberOfRecords, long totalBytes, int nullColumnCount, int columnCount, int rsFetchSize) {}
}
