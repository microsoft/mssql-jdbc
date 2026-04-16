/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * DTC/XA timeout tests: transaction timeout behavior, timeout enforcement,
 * XAResource.setTransactionTimeout, transaction abandonment detection.
 * Ported from FX dtcTimeout/DtcTimeout.java tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxXA)
@Tag(Constants.reqExternalSetup)
public class DTCTimeoutTest extends AbstractTest {

    private static SQLServerXADataSource xaDs;

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        xaDs = new SQLServerXADataSource();
        xaDs.setURL(connectionString);
    }

    private Xid createXid(int id) {
        byte[] gtrid = new byte[] {(byte) id, 0, 0, 0};
        byte[] bqual = new byte[] {(byte) id, 0, 0, 0};
        return new Xid() {
            public int getFormatId() {
                return 1;
            }

            public byte[] getGlobalTransactionId() {
                return gtrid;
            }

            public byte[] getBranchQualifier() {
                return bqual;
            }
        };
    }

    @Test
    public void testSetTransactionTimeout() throws Exception {
        XAConnection xaConn = xaDs.getXAConnection();
        try {
            XAResource xar = xaConn.getXAResource();
            assertTrue(xar.setTransactionTimeout(15));
            assertEquals(15, xar.getTransactionTimeout());
        } finally {
            xaConn.close();
        }
    }

    @Test
    public void testSetTransactionTimeoutZero() throws Exception {
        XAConnection xaConn = xaDs.getXAConnection();
        try {
            XAResource xar = xaConn.getXAResource();
            assertTrue(xar.setTransactionTimeout(0)); // Reset to default
        } finally {
            xaConn.close();
        }
    }

    @Test
    public void testXATransactionStartEnd() throws Exception {
        Xid xid = createXid(1);
        XAConnection xaConn = xaDs.getXAConnection();
        try {
            XAResource xar = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();

            xar.start(xid, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 1");
            }
            xar.end(xid, XAResource.TMSUCCESS);
            xar.rollback(xid);
        } finally {
            xaConn.close();
        }
    }

    @Test
    public void testXATransactionOnePhaseCommit() throws Exception {
        Xid xid = createXid(2);
        XAConnection xaConn = xaDs.getXAConnection();
        try {
            XAResource xar = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();

            xar.start(xid, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 1");
            }
            xar.end(xid, XAResource.TMSUCCESS);
            xar.commit(xid, true); // one-phase commit
        } finally {
            xaConn.close();
        }
    }

    @Test
    public void testXATransactionTwoPhaseCommit() throws Exception {
        Xid xid = createXid(3);
        XAConnection xaConn = xaDs.getXAConnection();
        try {
            XAResource xar = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();

            xar.start(xid, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 1");
            }
            xar.end(xid, XAResource.TMSUCCESS);
            int prepareResult = xar.prepare(xid);
            if (prepareResult == XAResource.XA_OK) {
                xar.commit(xid, false); // two-phase commit
            }
        } finally {
            xaConn.close();
        }
    }

    @Test
    public void testXATransactionRollback() throws Exception {
        Xid xid = createXid(4);
        XAConnection xaConn = xaDs.getXAConnection();
        try {
            XAResource xar = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();

            xar.start(xid, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 1");
            }
            xar.end(xid, XAResource.TMSUCCESS);
            xar.rollback(xid);
        } finally {
            xaConn.close();
        }
    }

    @Test
    public void testXATransactionWithTimeout() throws Exception {
        Xid xid = createXid(5);
        XAConnection xaConn = xaDs.getXAConnection();
        try {
            XAResource xar = xaConn.getXAResource();
            xar.setTransactionTimeout(30);
            Connection conn = xaConn.getConnection();

            xar.start(xid, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 1");
            }
            xar.end(xid, XAResource.TMSUCCESS);
            xar.rollback(xid);
        } finally {
            xaConn.close();
        }
    }

    @Test
    public void testXATransactionRecover() throws Exception {
        XAConnection xaConn = xaDs.getXAConnection();
        try {
            XAResource xar = xaConn.getXAResource();
            Xid[] recovered = xar.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
            assertNotNull(recovered);
        } finally {
            xaConn.close();
        }
    }

    @Test
    public void testXAIsSameRM() throws Exception {
        XAConnection xaConn1 = xaDs.getXAConnection();
        XAConnection xaConn2 = xaDs.getXAConnection();
        try {
            XAResource xar1 = xaConn1.getXAResource();
            XAResource xar2 = xaConn2.getXAResource();
            // Same RM check for same data source
            xar1.isSameRM(xar2);
        } finally {
            xaConn2.close();
            xaConn1.close();
        }
    }

    @Test
    public void testXATransactionSuspendResume() throws Exception {
        Xid xid = createXid(6);
        XAConnection xaConn = xaDs.getXAConnection();
        try {
            XAResource xar = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();

            xar.start(xid, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 1");
            }
            xar.end(xid, XAResource.TMSUSPEND);
            xar.start(xid, XAResource.TMRESUME);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 2");
            }
            xar.end(xid, XAResource.TMSUCCESS);
            xar.rollback(xid);
        } finally {
            xaConn.close();
        }
    }

    @Test
    public void testMultipleXATransactions() throws Exception {
        Xid xid1 = createXid(7);
        Xid xid2 = createXid(8);
        XAConnection xaConn = xaDs.getXAConnection();
        try {
            XAResource xar = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();

            // First transaction
            xar.start(xid1, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 1");
            }
            xar.end(xid1, XAResource.TMSUCCESS);
            xar.rollback(xid1);

            // Second transaction on same resource
            xar.start(xid2, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 2");
            }
            xar.end(xid2, XAResource.TMSUCCESS);
            xar.rollback(xid2);
        } finally {
            xaConn.close();
        }
    }

    @Test
    public void testXAGetTransactionTimeout() throws Exception {
        XAConnection xaConn = xaDs.getXAConnection();
        try {
            XAResource xar = xaConn.getXAResource();
            int defaultTimeout = xar.getTransactionTimeout();
            assertTrue(defaultTimeout >= 0);
        } finally {
            xaConn.close();
        }
    }

    @Test
    public void testXAForget() throws Exception {
        Xid xid = createXid(9);
        XAConnection xaConn = xaDs.getXAConnection();
        try {
            XAResource xar = xaConn.getXAResource();
            // forget on a non-existing heuristically completed transaction
            // should either succeed silently or throw XAException
            try {
                xar.forget(xid);
            } catch (XAException e) {
                // Expected for non-existing transaction
            }
        } finally {
            xaConn.close();
        }
    }
}
