/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.Utils;

@RunWith(JUnitPlatform.class)
public class BatchExecutionWithNullTest extends AbstractTest {

    static Statement stmt = null;
    static Connection connection = null;
    static PreparedStatement pstmt = null;
    static PreparedStatement pstmt1 = null;
    static ResultSet rs = null;

    /**
     * Test with combination of setString and setNull which cause the "Violation of PRIMARY KEY constraint and internally "Could not find prepared
     * statement with handle X" error.
     * 
     * @throws SQLException
     */
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

    /**
     * Tests the same as addBatch2, only with AE on the connection string
     * 
     * @throws SQLException
     */
    @Test
    public void testAddbatch2AEOnConnection() throws SQLException {
        connection = DriverManager.getConnection(connectionString + ";columnEncryptionSetting=Enabled;");
        testAddBatch2();
    }

    @BeforeEach
    public void testSetup() throws TestAbortedException, Exception {
        assumeTrue(13 <= new DBConnection(connectionString).getServerVersion(),
                "Aborting test case as SQL Server version is not compatible with Always encrypted ");

        connection = DriverManager.getConnection(connectionString);
        SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
        Utils.dropTableIfExists("esimple", stmt);
        String sql1 = "create table esimple (id integer not null, name varchar(255), constraint pk_esimple primary key (id))";
        stmt.execute(sql1);
        stmt.close();
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        connection = DriverManager.getConnection(connectionString);
        
        SQLServerStatement stmt = (SQLServerStatement) connection.createStatement();
        Utils.dropTableIfExists("esimple", stmt);

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
        if (null != connection) {
            connection.close();
        }
    }
}