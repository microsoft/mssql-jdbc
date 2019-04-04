package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@RunWith(JUnitPlatform.class)
public class TestActivityID extends AbstractTest {
    
    @Test
    public void testActivityID() throws Exception {
        int numExecution = 20;
        ExecutorService es = Executors.newFixedThreadPool(numExecution);
        CountDownLatch latch = new CountDownLatch(numExecution);
        for (int i = 0; i < numExecution; i++) {
            es.execute(new Runnable() {
                public void run() {
                    try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
                        stmt.execute("SELECT @@VERSION AS 'SQL Server Version'");
                    } catch (SQLException e) {
                        fail(e.toString());
                    }
                    latch.countDown();
                }
            });
        }
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
        HikariDataSource ds = new HikariDataSource(config);
        try {
            ExecutorService es = Executors.newFixedThreadPool(poolsize);
            CountDownLatch latchPool = new CountDownLatch(numPooledExecution);
            for (int i = 0; i < numPooledExecution; i++) {
                es.execute(new Runnable() {
                    public void run() {
                        try {
                            try (Connection con = ds.getConnection(); Statement stmt = con.createStatement()) {
                                stmt.execute("SELECT @@VERSION AS 'SQL Server Version'");
                            }
                        } catch (SQLException e) {
                            fail(e.toString());
                        }
                        latchPool.countDown();
                    }
                });
            }
            latchPool.await();
            es.shutdown();
        } finally {
            if (null != ds) {
                ds.close();
            }
        }

        // Clean up the entry that corresponds to HikariDataSource
        ActivityCorrelator.cleanupActivityId();
        assertEquals(0, ActivityCorrelator.getActivityIdTlsMap().size());
    }
    
    @BeforeAll
    public static void testSetup() throws Exception {
        String ActivityIDTraceOn = Util.ActivityIdTraceProperty + "=on";
        try (InputStream is = new ByteArrayInputStream(ActivityIDTraceOn.getBytes());) {
            LogManager lm = LogManager.getLogManager();
            lm.readConfiguration(is);
        }
    }
}

