/*  Copyright (c) 2022
 *  by Charles River Development, Inc., Burlington, MA
 *
 *  This software is furnished under a license and may be used only in
 *  accordance with the terms of such license.  This software may not be
 *  provided or otherwise made available to any other party.  No title to
 *  nor ownership of the software is hereby transferred.
 *
 *  This software is the intellectual property of Charles River Development, Inc.,
 *  and is protected by the copyright laws of the United States of America.
 *  All rights reserved internationally.
 *
 */
package com.microsoft.sqlserver.jdbc.PerfTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.sqlserver.jdbc.SQLServerResultSet;

/**
 * test program for JDBC driver performance comparison
 */
public class Main {
    private String driverUrl;
    private String userName;
    private String password;
    private enum DriverType {
        jTDS, Microsoft;
        int getResultSetType() {
            if (this == Microsoft) {
                return SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY;
            }
            return ResultSet.TYPE_FORWARD_ONLY;
        }
    }
    private DriverType driverType;

    /**
     * Usage: {@code Main <URL> <userName> [<password>]}
     * @param args program arguments
     */
    public static void main(String... args) throws Exception {
        new Main().readArgs(args).runTest();
    }

    public Main readArgs(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: Main <URL> <userName> [<password>]");
            System.exit(2);
            return this;
        }
        driverUrl = args[0];
        userName = args[1];
        if (args.length < 3) {
            System.out.println("enter password:");
            password = new String(System.console().readPassword());
        } else {
            password = args[2];
        }
        driverType = driverUrl.startsWith("jdbc:jtds:sqlserver") ? DriverType.jTDS : DriverType.Microsoft;
        return this;
    }

    private void runTest() throws Exception {
        //        System.out.println("------------------------------------------------------------");
        //        System.out.println(Main.class.getName() + " started at "+ new Date() + " for driver " + driverType);
        //        System.out.println("------------------------------------------------------------");
        long stopwatch = System.currentTimeMillis();

        try {
            Map<String, PerformanceTest.PerformanceResult> testResults = executeTest();
            printResults(testResults);
        } catch (SQLException e) {
            System.err.println("Test failed for driver " + driverType + ":" + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("------------------------------------------------------------");
        System.out.println(Main.class.getName() + " finished at "+ new Date() + " for driver " + driverType
                + " (total elapsed = " + (System.currentTimeMillis() - stopwatch) / 1000 + " seconds)");
        System.out.println("------------------------------------------------------------");
    }

    private Map<String, PerformanceTest.PerformanceResult> executeTest() throws SQLException, InterruptedException {
        try (Connection conn = createConnection()) {
            PreparePerfTables.preparePerfMultiplier(conn);
            PreparePerfTables.preparePerfTable(conn);
            int testNumber = 0;
            List<PerformanceTest> tests = getTests();
            Map<String, PerformanceTest.PerformanceResult> testResults = new LinkedHashMap<>();
            for (PerformanceTest test : tests) {
                testNumber++;
                //System.out.println("running " + test.getDescription() + "... (" + testNumber + "/" + tests.size() + ")");
                PerformanceTest.PerformanceResult result = test.run(conn, driverType.getResultSetType());
                testResults.put(test.getDescription(), result);
            }
            //Thread.sleep(1000000000);
            return testResults;
        }
    }

    private List<PerformanceTest> getTests() {
        return List.of(
                new PerfTablePerformanceTest(0)
                //new PerfTablePerformanceTest(512)
        );
    }

    public Connection createConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(driverUrl, userName, password);
        if (DriverType.jTDS == driverType) {
            // for jTDS we need to wrap the connection in order to adjust some of the return types like timestamps
            //return new CrdJtdsConnection(conn);
        }
        return conn;
    }

    private void printResults(Map<String, PerformanceTest.PerformanceResult> testResults) {
        System.out.println();
        System.out.printf("T# %-75s%-15s%-13s%-15s%-15s%-12s%-6s%n", "Performance Test", "Result Count", "Time(ms)", "Size", "nullCount", "columnCount", "fsize");
        int index = 1;
        for (Map.Entry<String, PerformanceTest.PerformanceResult> entry : testResults.entrySet()) {
            int length = entry.getKey().length();
            String description = entry.getKey().substring(0, Math.min(60, length))+".....";
            PerformanceTest.PerformanceResult result = entry.getValue();
            System.out.printf("%2d %-75s%-15s%-13s%-15s%-15s%-12s%-6s%n", index, description,
                    result.numberOfRecords(), result.elapsedMilliseconds(), result.totalBytes(),
                    result.nullColumnCount(), result.columnCount(), result.rsFetchSize());
            index++;
        }
        System.out.println();
    }
}
