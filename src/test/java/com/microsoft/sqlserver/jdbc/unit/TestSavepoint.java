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
import java.text.MessageFormat;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerSavepoint;
import com.microsoft.sqlserver.jdbc.TestResource;
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
        MessageFormat form = new MessageFormat(TestResource.getResource("R_savePointError"));
        Object[][] msgArgs = {{"Name", "same"}, {"Label", "Savepoint Name"}, {"SQLServerSavepoint.isNamed", "true"}};

        assertTrue(savePointName.equals(savePoint.getSavepointName()), form.format(msgArgs[0]));
        assertTrue(savePointName.equals(savePoint.getLabel()), form.format(msgArgs[1]));
        assertTrue(savePoint.isNamed(), form.format(msgArgs[2]));

        try {
            savePoint.getSavepointId();
            assertTrue(false, TestResource.getResource("R_expectedExceptionNotThrown"));
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

        MessageFormat form = new MessageFormat(TestResource.getResource("R_savePointError"));
        Object[][] msgArgs = {{"label", "not null"}, {"id", "not 0"}};
        assertNotNull(savePoint.getLabel(), form.format(msgArgs[0]));

        try {

            savePoint.getSavepointName();
            // Expecting Exception as trying to get SavePointname when we created savepoint without name
            assertTrue(false, TestResource.getResource("R_shouldThrowException"));
        }
        catch (SQLException e) {
        }

        assertTrue(savePoint.getSavepointId() != 0, form.format(msgArgs[1]));
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

        // SQLServerSavepoint.isNamed should be false as savePoint is created without name"
        assertFalse(savePoint.isNamed(), TestResource.getResource("R_shouldThrowException"));

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
            // Expecting Exception as can not set SetPoint when AutoCommit mode is set to true
            assertTrue(false, TestResource.getResource("R_shouldThrowException"));
        }
        catch (SQLException e) {
        }

    }

}
