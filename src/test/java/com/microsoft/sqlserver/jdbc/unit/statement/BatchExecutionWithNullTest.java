/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.Utils;

/**
 * This test demonstrate a strange bug, when alternate inserts with "setNull" and "setString" occur.
 * 
 * The test fails with "Violation of PRIMARY KEY constraint. The duplicate key value is (47)"
 *  
 * I've investigated a lot of time to provide a test case sample:
 * <ul>
 * <li>you must use "setNull" & "setString" in alternation (setString(x, null) does work)</li>
 * <li>the "Violation of PRIMARY KEY constraint" is not the real error, the real error is
 * <b>Could not find prepared statement with handle X</b></li>
 * <li>If 'columnEncryptionSetting' is enabled, it does not occur</li>
 * <li>The problem is probably somewhere in com.microsoft.sqlserver.jdbc.Parameter line 732.</li>
 * </ul>
 * 
 * When the debug log is enabled, you can see that the datatype changes from
 * <b>n</b>varchar(4000) to varchar(8000):
 * 
 * If you look on the wire protocol, you'll see
 * <pre>
 * 03 01 01 03 00 3A 01 00 16 00 00 00 12 00 00 00   .....:..........
 * 02 00 00 00 00 00 00 00 00 00 01 00 00 00 FF FF   ................
 * 0A 00 00 00 00 00 E7 40 1F 09 04 D0 00 34 80 00   .......@.....4..
 * 69 00 6E 00 73 00 65 00 72 00 74 00 20 00 69 00   i.n.s.e.r.t. .i.
 * 6E 00 74 00 6F 00 20 00 65 00 73 00 69 00 6D 00   n.t.o. .e.s.i.m.
 * 70 00 6C 00 65 00 20 00 28 00 69 00 64 00 2C 00   p.l.e. .(.i.d.,.
 * 20 00 6E 00 61 00 6D 00 65 00 29 00 20 00 76 00    .n.a.m.e.). .v.
 * 61 00 6C 00 75 00 65 00 73 00 20 00 28 00 40 00   a.l.u.e.s. .(.@.
 * 50 00 30 00 2C 00 20 00 40 00 50 00 31 00 29 00   P.0.,. .@.P.1.).
 * 20 00 20 00 20 00 20 00 20 00 20 00 20 00 20 00    . . . . . . . .
 * 20 00 20 00 20 00 20 00 20 00 20 00 20 00 20 00    . . . . . . . .
 * 00 00 E7 40 1F 09 04 D0 00 34 32 00 40 00 50 00   ...@.....42.@.P.
 * 30 00 20 00 69 00 6E 00 74 00 2C 00 40 00 50 00   0. .i.n.t.,.@.P.
 * 31 00 20 00 76 00 61 00 72 00 63 00 68 00 61 00   1. .v.a.r.c.h.a.  &lt-- p1=varchar(8000)
 * 72 00 28 00 38 00 30 00 30 00 30 00 29 00 00 00   r.(.8.0.0.0.)...
 * 26 04 04 2A 00 00 00 00 00 A7 40 1F 09 04 D0 00   &..*......@.....
 * 34 FF FF                                          4..
 * </pre>
 * second packet:
 * <pre>
 * 03 01 01 14 00 3A 01 00 16 00 00 00 12 00 00 00   .....:..........
 * 02 00 00 00 00 00 00 00 00 00 01 00 00 00 FF FF   ................
 * 0D 00 00 00 00 01 26 04 04 00 00 00 00 00 00 E7   ......&.........
 * 40 1F 09 04 D0 00 34 34 00 40 00 50 00 30 00 20   @.....44.@.P.0. 
 * 00 69 00 6E 00 74 00 2C 00 40 00 50 00 31 00 20   .i.n.t.,.@.P.1. 
 * 00 6E 00 76 00 61 00 72 00 63 00 68 00 61 00 72   .n.v.a.r.c.h.a.r  &lt-- p1=nvarchar(4000)
 * 00 28 00 34 00 30 00 30 00 30 00 29 00 00 00 E7   .(.4.0.0.0.)....
 * 40 1F 09 04 D0 00 34 80 00 69 00 6E 00 73 00 65   @.....4..i.n.s.e
 * 00 72 00 74 00 20 00 69 00 6E 00 74 00 6F 00 20   .r.t. .i.n.t.o. 
 * 00 65 00 73 00 69 00 6D 00 70 00 6C 00 65 00 20   .e.s.i.m.p.l.e. 
 * 00 28 00 69 00 64 00 2C 00 20 00 6E 00 61 00 6D   .(.i.d.,. .n.a.m
 * 00 65 00 29 00 20 00 76 00 61 00 6C 00 75 00 65   .e.). .v.a.l.u.e
 * 00 73 00 20 00 28 00 40 00 50 00 30 00 2C 00 20   .s. .(.@.P.0.,. 
 * 00 40 00 50 00 31 00 29 00 20 00 20 00 20 00 20   .@.P.1.). . . . 
 * 00 20 00 20 00 20 00 20 00 20 00 20 00 20 00 20   . . . . . . . . 
 * 00 20 00 20 00 20 00 20 00 00 00 26 04 04 2B 00   . . . . ...&..+.
 * 00 00 00 00 E7 40 1F 09 04 D0 00 34 06 00 46 00   .....@.....4..F.
 * 4F 00 4F 00                                       O.O.
 * </pre>
 * third packet
 * <pre>
 * 03 01 01 0C 00 3A 01 00 16 00 00 00 12 00 00 00   .....:..........
 * 02 00 00 00 00 00 00 00 00 00 01 00 00 00 FF FF   ................
 * 0D 00 00 00 00 01 26 04 04 01 00 00 00 00 00 E7   ......&.........
 * 40 1F 09 04 D0 00 34 32 00 40 00 50 00 30 00 20   @.....42.@.P.0. 
 * 00 69 00 6E 00 74 00 2C 00 40 00 50 00 31 00 20   .i.n.t.,.@.P.1. 
 * 00 76 00 61 00 72 00 63 00 68 00 61 00 72 00 28   .v.a.r.c.h.a.r.(  &lt-- p1=varchar(8000)
 * 00 38 00 30 00 30 00 30 00 29 00 00 00 E7 40 1F   .8.0.0.0.)....@.
 * 09 04 D0 00 34 80 00 69 00 6E 00 73 00 65 00 72   ....4..i.n.s.e.r
 * 00 74 00 20 00 69 00 6E 00 74 00 6F 00 20 00 65   .t. .i.n.t.o. .e
 * 00 73 00 69 00 6D 00 70 00 6C 00 65 00 20 00 28   .s.i.m.p.l.e. .(
 * 00 69 00 64 00 2C 00 20 00 6E 00 61 00 6D 00 65   .i.d.,. .n.a.m.e
 * 00 29 00 20 00 76 00 61 00 6C 00 75 00 65 00 73   .). .v.a.l.u.e.s
 * 00 20 00 28 00 40 00 50 00 30 00 2C 00 20 00 40   . .(.@.P.0.,. .@
 * 00 50 00 31 00 29 00 20 00 20 00 20 00 20 00 20   .P.1.). . . . . 
 * 00 20 00 20 00 20 00 20 00 20 00 20 00 20 00 20   . . . . . . . . 
 * 00 20 00 20 00 20 00 00 00 26 04 04 2C 00 00 00   . . . ...&..,...
 * 00 00 A7 40 1F 09 04 D0 00 34 FF FF               ...@.....4..
 * </pre>
 * After some more packets the server responds with
 * <pre>
 * 04 01 00 9E 00 3A 01 00 AA 86 00 F3 1F 00 00 01   .....:..........
 * 10 30 00 43 00 6F 00 75 00 6C 00 64 00 20 00 6E   .0.C.o.u.l.d. .n
 * 00 6F 00 74 00 20 00 66 00 69 00 6E 00 64 00 20   .o.t. .f.i.n.d. 
 * 00 70 00 72 00 65 00 70 00 61 00 72 00 65 00 64   .p.r.e.p.a.r.e.d
 * 00 20 00 73 00 74 00 61 00 74 00 65 00 6D 00 65   . .s.t.a.t.e.m.e
 * 00 6E 00 74 00 20 00 77 00 69 00 74 00 68 00 20   .n.t. .w.i.t.h. 
 * 00 68 00 61 00 6E 00 64 00 6C 00 65 00 20 00 31   .h.a.n.d.l.e. .1
 * 00 2E 00 0C 35 00 37 00 38 00 66 00 39 00 31 00   ....5.7.8.f.9.1.
 * 34 00 32 00 31 00 38 00 39 00 65 00 00 01 00 00   4.2.1.8.9.e.....
 * 00 FE 02 00 E0 00 00 00 00 00 00 00 00 00         ..............
 * </pre>
 * and it seems that the JDBC driver tries to submit the last insert statement again,
 * which leads into a duplicate key exception.
 * 
 *
 */
@RunWith(JUnitPlatform.class)
public class BatchExecutionWithNullTest extends AbstractTest {

    static Statement stmt = null;
    static Connection connection = null;
    static PreparedStatement pstmt = null;
    static PreparedStatement pstmt1 = null;
    static ResultSet rs = null;

    @Test
    public void testAddBatch2() throws SQLException {
        // try {
        String sPrepStmt = "insert into esimple (id, name) values (?, ?)";
        int updateCountlen = 0;
        int key = 42;
        
        // this is the minimum sequence, I've found to trigger the error
        pstmt = connection.prepareStatement(sPrepStmt);
        pstmt.setInt(1, key++);
        pstmt.setNull(2, Types.VARCHAR);
        pstmt.addBatch();
        
        pstmt.setInt(1, key++);
        pstmt.setString(2, "FOO");
        pstmt.addBatch();
        
        pstmt.setInt(1, key++);
        pstmt.setNull(2, Types.VARCHAR);
        pstmt.addBatch();
        
        int[] updateCount = pstmt.executeBatch();
        updateCountlen += updateCount.length;
              
        pstmt.setInt(1, key++);
        pstmt.setString(2, "BAR");
        pstmt.addBatch();
        
        pstmt.setInt(1, key++);
        pstmt.setNull(2, Types.VARCHAR);
        pstmt.addBatch();
        
        updateCount = pstmt.executeBatch();
        updateCountlen += updateCount.length;
 
        assertTrue(updateCountlen == 5, "addBatch does not add the SQL Statements to Batch ,call to addBatch failed");

        String sPrepStmt1 = "select count(*) from esimple";

        pstmt1 = connection.prepareStatement(sPrepStmt1);
        rs = pstmt1.executeQuery();
        rs.next();
        assertTrue(rs.getInt(1) == 5, "affected rows does not match with batch size. Insert failed");
        pstmt1.close();

    }

    @BeforeAll
    public static void testSetup() throws TestAbortedException, Exception {
        assumeTrue(13 <= new DBConnection(connectionString).getServerVersion(),
                "Aborting test case as SQL Server version is not compatible with Always encrypted ");
        
        //connection = DriverManager.getConnection(connectionString + ";columnEncryptionSetting=Enabled;");
        //error occurs only if columnEncryptionSetting is disabled
        connection = DriverManager.getConnection(connectionString);
        SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
        Utils.dropTableIfExists("esimple", stmt);
        String sql1 = "create table esimple (id integer not null, name varchar(255), constraint pk_esimple primary key (id))";
        stmt.execute(sql1);
        stmt.close();
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {

        SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
        Utils.dropTableIfExists("esimple", stmt);

        if (null != connection) {
            connection.close();
        }
        if (null != pstmt) {
            pstmt.close();
        }
        if (null != pstmt1) {
            pstmt1.close();
        }
        if (null != stmt) {
            stmt.close();
        }
        if (null != rs) {
            rs.close();
        }
    }
}