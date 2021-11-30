/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;
import java.util.stream.IntStream;

import javax.sql.PooledConnection;

import org.junit.ComparisonFailure;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


@RunWith(JUnitPlatform.class)
public class ActivityIDTest extends AbstractTest {

    @Test
    public void testActivityID() throws Exception {
        int numExecution = 20;
        ExecutorService es = Executors.newFixedThreadPool(numExecution);
        CountDownLatch latch = new CountDownLatch(numExecution);
        es.execute(() -> {
            IntStream.range(0, numExecution).forEach(i -> {
                try (Connection c = getConnection(); Statement s = c.createStatement();
                        ResultSet rs = s.executeQuery("SELECT @@VERSION AS 'SQL Server Version'")) {
                    while (rs.next()) {
                        rs.getString(1);
                    }
                } catch (SQLException e) {
                    fail(e.toString());
                }
                latch.countDown();
            });
        });
        latch.await();
        es.shutdown();
        assertEquals(0, ActivityCorrelator.getActivityIdTlsMap().size());
    }

    @Test
    public void testActivityIDPooled() throws Exception {
        int poolsize = 10;
        int numPooledExecution = 200;

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(connectionString);
        config.setMaximumPoolSize(poolsize);
        ExecutorService es = Executors.newFixedThreadPool(poolsize);
        CountDownLatch latchPoolOuterThread = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            CountDownLatch latchPool = new CountDownLatch(numPooledExecution);

            public void run() {
                HikariDataSource ds = new HikariDataSource(config);
                es.execute(() -> {
                    IntStream.range(0, numPooledExecution).forEach(i -> {
                        try (Connection c = ds.getConnection(); Statement s = c.createStatement();
                                ResultSet rs = s.executeQuery("SELECT @@VERSION AS 'SQL Server Version'")) {
                            while (rs.next()) {
                                rs.getString(1);
                            }
                        } catch (SQLException e) {
                            fail(e.toString());
                        }
                        latchPool.countDown();
                    });
                });
                try {
                    latchPool.await();
                } catch (InterruptedException e) {
                    fail(e.toString());
                } finally {
                    if (null != ds) {
                        es.shutdown();
                        ds.close();
                    }
                }
                latchPoolOuterThread.countDown();
            }
        });
        t.run();
        latchPoolOuterThread.await();

        try {
            try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
                stmt.execute("SELECT @@VERSION AS 'SQL Server Version'");
            }
        } catch (SQLException e) {
            fail(e.toString());
        }

        try {
            assertEquals(0, ActivityCorrelator.getActivityIdTlsMap().size());
        } catch (ComparisonFailure e) {
            Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
            System.out.println("List of threads alive:");
            for (Thread thread : threadSet) {
                System.out.println(thread.toString());
            }
            System.out.println("List of entries in the ActivityID map:");
            System.out.println(ActivityCorrelator.getActivityIdTlsMap().toString());
            fail(e.toString());
        }
    }

    @Test
    public void testActivityIDPooledConnection() throws Exception {
        int poolsize = 10;
        int numPooledExecution = 200;

        PooledConnection pooledCon = ((SQLServerConnectionPoolDataSource) dsPool).getPooledConnection();
        ExecutorService es = Executors.newFixedThreadPool(poolsize);
        try {
            CountDownLatch latchPool = new CountDownLatch(numPooledExecution);
            es.execute(() -> {
                IntStream.range(0, numPooledExecution).forEach(i -> {
                    try (Connection c = pooledCon.getConnection(); Statement s = c.createStatement(); ResultSet rs = s
                            .executeQuery("SELECT @@VERSION AS 'SQL Server Version'")) {} catch (SQLException e) {
                        fail(e.toString());
                    }
                    latchPool.countDown();
                });
            });
            latchPool.await();
        } finally {
            es.shutdown();
            pooledCon.close();
        }
        assertEquals(0, ActivityCorrelator.getActivityIdTlsMap().size());
    }

    @AfterAll
    public static void teardown() throws Exception {
        String activityIDTraceOff = Util.ACTIVITY_ID_TRACE_PROPERTY + "=off";
        try (InputStream is = new ByteArrayInputStream(activityIDTraceOff.getBytes());) {
            LogManager lm = LogManager.getLogManager();
            lm.readConfiguration(is);
        }
    }

    @BeforeAll
    public static void testSetup() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();

        String activityIDTraceOn = Util.ACTIVITY_ID_TRACE_PROPERTY + "=on";
        try (InputStream is = new ByteArrayInputStream(activityIDTraceOn.getBytes());) {
            LogManager lm = LogManager.getLogManager();
            lm.readConfiguration(is);
        }
    }
}
