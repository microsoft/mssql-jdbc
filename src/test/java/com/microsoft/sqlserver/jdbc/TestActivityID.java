package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;

import javax.sql.PooledConnection;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class TestActivityID extends AbstractTest {
    
    @Test
    public void testActivityID() throws Exception {
        // Without pooling
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
        assertEquals(0, ActivityCorrelator.getActivityIdTlsMap().size());
        
        // With pooling
        int poolsize = 10;
        int numPooledExecution = 200;
        PooledConnection pcon = ((SQLServerConnectionPoolDataSource) dsPool).getPooledConnection();
        es = Executors.newFixedThreadPool(poolsize);
        CountDownLatch latchPool = new CountDownLatch(numPooledExecution);
        for (int i = 0; i < numPooledExecution; i++) {
            es.execute(new Runnable() {
                public void run() {
                    try (Connection con = pcon.getConnection(); Statement stmt = con.createStatement()) {
                        stmt.execute("SELECT @@VERSION AS 'SQL Server Version'");
                    } catch (SQLException e) {
                        fail(e.toString());
                    }
                    latchPool.countDown();
                }
            });
        }
        latchPool.await();
        // we expect `poolsize` entries in the map left at the end, for the pooled connections that haven't been closed.
        assertEquals(poolsize, ActivityCorrelator.getActivityIdTlsMap().size());
    }
    
    @BeforeAll
    public static void testSetup() throws Exception {
        try (InputStream is = new FileInputStream("src\\test\\resources\\logging.properties")) {
            LogManager lm = LogManager.getLogManager();
            lm.readConfiguration(is);
        }
    }
}

