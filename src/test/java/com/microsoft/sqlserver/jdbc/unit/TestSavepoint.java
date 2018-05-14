/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerSavepoint;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

/**
 * Unit test case for Creating SavePoint.
 */
@RunWith(JUnitPlatform.class)
public class TestSavepoint extends AbstractTest {

    Connection connection = null;
    Statement statement = null;
    String savePointName = RandomUtil.getIdentifier("SavePoint", 31, true, false);

    /**
     * Testing SavePoint with name.
     */
    @Test
    public void testSavePointName() throws SQLException {
        connection = DriverManager.getConnection(connectionString);

        connection.setAutoCommit(false);

        SQLServerSavepoint savePoint = (SQLServerSavepoint) connection.setSavepoint(savePointName);
        assertTrue(savePointName.equals(savePoint.getSavepointName()), "Savepoint Name should be same.");

        assertTrue(savePointName.equals(savePoint.getLabel()), "Savepoint Label should be same as Savepoint  Name.");

        assertTrue(savePoint.isNamed(), "SQLServerSavepoint.isNamed should be true");
        try {
            savePoint.getSavepointId();
            assertTrue(false, "Expecting Exception as trying to get SavePointId when we created savepoint with name");
        }
        catch (SQLException e) {
        }

        connection.rollback();
    }

    /**
     * Testing SavePoint without name.
     * 
     * @throws SQLException
     */
    @Test
    public void testSavePointId() throws SQLException {
        connection = DriverManager.getConnection(connectionString);

        connection.setAutoCommit(false);

        SQLServerSavepoint savePoint = (SQLServerSavepoint) connection.setSavepoint(null);
        assertNotNull(savePoint.getLabel(), "Savepoint Label should not be null.");

        try {
            savePoint.getSavepointName();
            assertTrue(false, "Expecting Exception as trying to get SavePointname when we created savepoint without name");
        }
        catch (SQLException e) {
        }

        assertTrue(savePoint.getSavepointId() != 0, "SavePoint should not be 0");
        connection.rollback();
    }

    /**
     * Testing SavePoint without name.
     * 
     * @throws SQLException
     */
    @Test
    public void testSavePointIsNamed() throws SQLException {
        connection = DriverManager.getConnection(connectionString);

        connection.setAutoCommit(false);

        SQLServerSavepoint savePoint = (SQLServerSavepoint) connection.setSavepoint(null);

        assertFalse(savePoint.isNamed(), "SQLServerSavepoint.isNamed should be false as savePoint is created without name");

        connection.rollback();
    }

    /**
     * Test SavePoint when auto commit is true.
     * 
     * @throws SQLException
     */
    @Test
    public void testSavePointWithAutoCommit() throws SQLException {
        connection = DriverManager.getConnection(connectionString);

        connection.setAutoCommit(true);

        try {
            connection.setSavepoint(null);
            assertTrue(false, "Expecting Exception as can not set SetPoint when AutoCommit mode is set to true.");
        }
        catch (SQLException e) {
        }

    }

}
