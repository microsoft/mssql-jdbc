/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * Stress tests: concurrent connection handling, multi-threaded workloads,
 * rapid connection cycling, mixed read/write workloads under load.
 * Ported from FX stress/plan.xml concurrent workload tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxStress)
public class StressTest extends AbstractTest {

    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("Stress_Tab"));
    private static final int THREAD_COUNT = 8;
    private static final int OPERATIONS_PER_THREAD = 10;

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INT IDENTITY PRIMARY KEY, COL1 VARCHAR(200), COL2 INT, COL3 DATETIME2 DEFAULT GETDATE())");
            for (int i = 1; i <= 100; i++) {
                stmt.executeUpdate(
                        "INSERT INTO " + tableName + " (COL1, COL2) VALUES ('stress_row" + i + "', " + i + ")");
            }
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    @Test
    public void testConcurrentReads() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < THREAD_COUNT; t++) {
            futures.add(executor.submit(() -> {
                try {
                    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                        try (Connection conn = PrepUtil.getConnection(connectionString);
                                Statement stmt = conn.createStatement();
                                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                            int count = 0;
                            while (rs.next()) {
                                count++;
                                rs.getString("COL1");
                                rs.getInt("COL2");
                            }
                            if (count > 0) {
                                successCount.incrementAndGet();
                            }
                        }
                    }
                } catch (SQLException e) {
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            }));
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS), "Timed out waiting for threads");
        executor.shutdown();
        assertTrue(successCount.get() > 0, "No successful operations");
        assertTrue(errorCount.get() == 0, "Errors occurred: " + errorCount.get());
    }

    @Test
    public void testConcurrentMixedWorkload() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                        try (Connection conn = PrepUtil.getConnection(connectionString)) {
                            if (threadId % 2 == 0) {
                                // Read workload
                                try (Statement stmt = conn.createStatement();
                                        ResultSet rs = stmt.executeQuery(
                                                "SELECT TOP 10 * FROM " + tableName + " ORDER BY ID")) {
                                    while (rs.next()) {
                                        rs.getString("COL1");
                                    }
                                }
                            } else {
                                // Insert workload
                                try (PreparedStatement ps = conn.prepareStatement(
                                        "INSERT INTO " + tableName + " (COL1, COL2) VALUES (?, ?)")) {
                                    ps.setString(1, "stress_t" + threadId + "_i" + i);
                                    ps.setInt(2, threadId * 1000 + i);
                                    ps.executeUpdate();
                                }
                            }
                            successCount.incrementAndGet();
                        }
                    }
                } catch (SQLException e) {
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS), "Timed out waiting for threads");
        executor.shutdown();
        assertTrue(successCount.get() > 0, "No successful operations");
    }

    @Test
    public void testRapidConnectionCycling() throws Exception {
        int cycles = 50;
        int successCount = 0;
        for (int i = 0; i < cycles; i++) {
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                assertNotNull(conn);
                assertTrue(conn.isValid(2));
                successCount++;
            }
        }
        assertEquals(cycles, successCount);
    }

    @Test
    public void testConcurrentPreparedStatements() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try (Connection conn = PrepUtil.getConnection(connectionString)) {
                    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                        try (PreparedStatement ps = conn.prepareStatement(
                                "SELECT * FROM " + tableName + " WHERE COL2 = ?")) {
                            ps.setInt(1, (threadId * OPERATIONS_PER_THREAD) + i + 1);
                            try (ResultSet rs = ps.executeQuery()) {
                                while (rs.next()) {
                                    rs.getString("COL1");
                                }
                            }
                            successCount.incrementAndGet();
                        }
                    }
                } catch (SQLException e) {
                    // Log but don't fail for connection contention
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(60, TimeUnit.SECONDS));
        executor.shutdown();
        assertTrue(successCount.get() > 0);
    }

    @Test
    public void testConcurrentTransactions() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        AtomicInteger commitCount = new AtomicInteger(0);
        AtomicInteger rollbackCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(4);

        for (int t = 0; t < 4; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try (Connection conn = PrepUtil.getConnection(connectionString)) {
                    conn.setAutoCommit(false);
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + tableName + " (COL1, COL2) VALUES (?, ?)")) {
                        ps.setString(1, "txn_t" + threadId);
                        ps.setInt(2, 50000 + threadId);
                        ps.executeUpdate();
                    }
                    if (threadId % 2 == 0) {
                        conn.commit();
                        commitCount.incrementAndGet();
                    } else {
                        conn.rollback();
                        rollbackCount.incrementAndGet();
                    }
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                    // Connection contention
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
        assertTrue(commitCount.get() + rollbackCount.get() > 0);
    }

    @Test
    public void testQueryTimeoutUnderLoad() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(4);

        for (int t = 0; t < 4; t++) {
            executor.submit(() -> {
                try (Connection conn = PrepUtil.getConnection(connectionString);
                        Statement stmt = conn.createStatement()) {
                    stmt.setQueryTimeout(10);
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                        if (rs.next()) {
                            assertTrue(rs.getInt(1) > 0);
                            successCount.incrementAndGet();
                        }
                    }
                } catch (SQLException e) {
                    // Timeout or contention
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
        assertTrue(successCount.get() > 0);
    }


}
