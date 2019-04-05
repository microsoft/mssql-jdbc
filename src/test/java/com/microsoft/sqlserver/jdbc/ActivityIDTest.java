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

import javax.sql.PooledConnection;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
        ExecutorService es = Executors.newFixedThreadPool(poolsize);
        CountDownLatch latchPoolOuterThread = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            CountDownLatch latchPool = new CountDownLatch(numPooledExecution);
            public void run() {
                HikariDataSource ds = new HikariDataSource(config);
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
        assertEquals(0, ActivityCorrelator.getActivityIdTlsMap().size());
    }
    
    @Test
    public void testActivityIDPooledConnection() throws Exception {
        int poolsize = 10;
        int numPooledExecution = 200;

        PooledConnection pooledCon = ((SQLServerConnectionPoolDataSource) dsPool).getPooledConnection();
        ExecutorService es = Executors.newFixedThreadPool(poolsize);
        try {
            CountDownLatch latchPool = new CountDownLatch(numPooledExecution);
            es.execute(new Runnable() {
                public void run() {
                    for (int i = 0; i < numPooledExecution; i++) {
                        try (Connection con = pooledCon.getConnection(); Statement stmt = con.createStatement()) {
                            stmt.execute("SELECT @@VERSION AS 'SQL Server Version'");
                        } catch (SQLException e) {
                            fail(e.toString());
                        }
                        latchPool.countDown();
                    }
                }
            });
            latchPool.await();
        } finally {
            es.shutdown();
            pooledCon.close();
        }
        assertEquals(0, ActivityCorrelator.getActivityIdTlsMap().size());
    }
    
    @BeforeEach
    @AfterEach
    public void clearActivityId() {
        ActivityCorrelator.clear();
    }
    
    @AfterAll
    public static void teardown() throws Exception {
        String ActivityIDTraceOff = Util.ActivityIdTraceProperty + "=off";
        try (InputStream is = new ByteArrayInputStream(ActivityIDTraceOff.getBytes());) {
            LogManager lm = LogManager.getLogManager();
            lm.readConfiguration(is);
        }
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

