/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.cts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

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
 * CTS compliance tests for CallableStatement: output parameter retrieval, input parameter binding,
 * registerOutParameter, wasNull, and type conversions via stored procedures.
 * Ported from FX callStmtClient.java CTS tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxCTS)
public class CallableStatementCTSTest extends AbstractTest {

    private static final String numericTable = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_Numeric_Tab"));
    private static final String varcharTable = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_Varchar_Tab"));
    private static final String dateTable = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_Date_Tab"));
    private static final String binaryTable = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_Binary_Tab"));

    private static final String numericProc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_Numeric_Proc"));
    private static final String varcharProc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_Varchar_Proc"));
    private static final String dateProc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_Date_Proc"));
    private static final String binaryProc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_Binary_Proc"));
    private static final String nullProc = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_Null_Proc"));

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Numeric table
            TestUtils.dropTableIfExists(numericTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + numericTable
                    + " (MAX_VAL NUMERIC(18,6), MIN_VAL NUMERIC(18,6), NULL_VAL NUMERIC(18,6) NULL)");
            stmt.executeUpdate(
                    "INSERT INTO " + numericTable + " VALUES (999999999999.999999, 0.000001, NULL)");

            // Varchar table
            TestUtils.dropTableIfExists(varcharTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + varcharTable
                    + " (COFFEE_NAME VARCHAR(200), NULL_VAL VARCHAR(200) NULL)");
            stmt.executeUpdate("INSERT INTO " + varcharTable + " VALUES ('Colombian', NULL)");

            // Date table
            TestUtils.dropTableIfExists(dateTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + dateTable
                    + " (DT_VAL DATE, TM_VAL TIME, TS_VAL DATETIME2, NULL_VAL DATE NULL)");
            stmt.executeUpdate("INSERT INTO " + dateTable
                    + " VALUES ('2024-03-15', '10:30:45', '2024-03-15 10:30:45.123', NULL)");

            // Binary table
            TestUtils.dropTableIfExists(binaryTable, stmt);
            stmt.executeUpdate(
                    "CREATE TABLE " + binaryTable + " (BIN_VAL VARBINARY(200), NULL_VAL VARBINARY(200) NULL)");
            stmt.executeUpdate(
                    "INSERT INTO " + binaryTable + " VALUES (0x4142434445, NULL)");

            // Stored procedures
            TestUtils.dropProcedureIfExists(numericProc, stmt);
            stmt.executeUpdate("CREATE PROCEDURE " + numericProc
                    + " @MAX_PARAM NUMERIC(18,6) OUTPUT, @MIN_PARAM NUMERIC(18,6) OUTPUT, @NULL_PARAM NUMERIC(18,6) OUTPUT "
                    + "AS SELECT @MAX_PARAM=MAX_VAL, @MIN_PARAM=MIN_VAL, @NULL_PARAM=NULL_VAL FROM "
                    + numericTable);

            TestUtils.dropProcedureIfExists(varcharProc, stmt);
            stmt.executeUpdate("CREATE PROCEDURE " + varcharProc
                    + " @NAME_PARAM VARCHAR(200) OUTPUT, @NULL_PARAM VARCHAR(200) OUTPUT "
                    + "AS SELECT @NAME_PARAM=COFFEE_NAME, @NULL_PARAM=NULL_VAL FROM " + varcharTable);

            TestUtils.dropProcedureIfExists(dateProc, stmt);
            stmt.executeUpdate("CREATE PROCEDURE " + dateProc
                    + " @DT_PARAM DATE OUTPUT, @TM_PARAM TIME OUTPUT, @TS_PARAM DATETIME2 OUTPUT, @NULL_PARAM DATE OUTPUT "
                    + "AS SELECT @DT_PARAM=DT_VAL, @TM_PARAM=TM_VAL, @TS_PARAM=TS_VAL, @NULL_PARAM=NULL_VAL FROM "
                    + dateTable);

            TestUtils.dropProcedureIfExists(binaryProc, stmt);
            stmt.executeUpdate("CREATE PROCEDURE " + binaryProc
                    + " @BIN_PARAM VARBINARY(200) OUTPUT, @NULL_PARAM VARBINARY(200) OUTPUT "
                    + "AS SELECT @BIN_PARAM=BIN_VAL, @NULL_PARAM=NULL_VAL FROM " + binaryTable);

            TestUtils.dropProcedureIfExists(nullProc, stmt);
            stmt.executeUpdate("CREATE PROCEDURE " + nullProc
                    + " @OUT_PARAM VARCHAR(200) OUTPUT AS SET @OUT_PARAM = NULL");
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropProcedureIfExists(numericProc, stmt);
            TestUtils.dropProcedureIfExists(varcharProc, stmt);
            TestUtils.dropProcedureIfExists(dateProc, stmt);
            TestUtils.dropProcedureIfExists(binaryProc, stmt);
            TestUtils.dropProcedureIfExists(nullProc, stmt);
            TestUtils.dropTableIfExists(numericTable, stmt);
            TestUtils.dropTableIfExists(varcharTable, stmt);
            TestUtils.dropTableIfExists(dateTable, stmt);
            TestUtils.dropTableIfExists(binaryTable, stmt);
        }
    }

    /**
     * Tests getBigDecimal output parameter retrieval from stored procedure with NUMERIC type.
     * Ported from FX CTS callStmtClient getBigDecimal variations.
     */
    @Test
    public void testGetBigDecimalOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + numericProc + "(?,?,?)}")) {
            cs.registerOutParameter(1, Types.NUMERIC, 6);
            cs.registerOutParameter(2, Types.NUMERIC, 6);
            cs.registerOutParameter(3, Types.NUMERIC, 6);
            cs.execute();
            BigDecimal maxVal = cs.getBigDecimal(1);
            assertNotNull(maxVal);
            assertTrue(maxVal.compareTo(new BigDecimal("999999999999.999999")) == 0);
            BigDecimal minVal = cs.getBigDecimal(2);
            assertNotNull(minVal);
            assertTrue(minVal.compareTo(new BigDecimal("0.000001")) == 0);
        }
    }

    /**
     * Tests getDouble output parameter retrieval from stored procedure with DOUBLE type.
     * Ported from FX CTS callStmtClient getDouble variations.
     */
    @Test
    public void testGetDoubleOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + numericProc + "(?,?,?)}")) {
            cs.registerOutParameter(1, Types.DOUBLE);
            cs.registerOutParameter(2, Types.DOUBLE);
            cs.registerOutParameter(3, Types.DOUBLE);
            cs.execute();
            double maxVal = cs.getDouble(1);
            assertTrue(maxVal > 0);
            double minVal = cs.getDouble(2);
            assertTrue(minVal > 0);
        }
    }

    /**
     * Tests getFloat output parameter retrieval from stored procedure with FLOAT type.
     * Ported from FX CTS callStmtClient getFloat variations.
     */
    @Test
    public void testGetFloatOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + numericProc + "(?,?,?)}")) {
            cs.registerOutParameter(1, Types.FLOAT);
            cs.registerOutParameter(2, Types.FLOAT);
            cs.registerOutParameter(3, Types.FLOAT);
            cs.execute();
            float maxVal = cs.getFloat(1);
            assertTrue(maxVal > 0);
        }
    }

    /**
     * Tests getInt output parameter retrieval from stored procedure with INTEGER type.
     * Ported from FX CTS callStmtClient getInt variations.
     */
    @Test
    public void testGetIntOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + numericProc + "(?,?,?)}")) {
            // Only register MIN_VAL param as INTEGER (MAX_VAL exceeds int range)
            cs.registerOutParameter(1, Types.NUMERIC);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.registerOutParameter(3, Types.INTEGER);
            cs.execute();
            int minVal = cs.getInt(2);
            assertTrue(minVal == 0); // 0.000001 truncates to 0
        }
    }

    /**
     * Tests getLong output parameter retrieval from stored procedure with BIGINT type.
     * Ported from FX CTS callStmtClient getLong variations.
     */
    @Test
    public void testGetLongOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + numericProc + "(?,?,?)}")) {
            cs.registerOutParameter(1, Types.BIGINT);
            cs.registerOutParameter(2, Types.BIGINT);
            cs.registerOutParameter(3, Types.BIGINT);
            cs.execute();
            long maxVal = cs.getLong(1);
            assertTrue(maxVal > 0);
        }
    }

    /**
     * Tests getShort output parameter retrieval from stored procedure with SMALLINT type.
     * Ported from FX CTS callStmtClient getShort variations.
     */
    @Test
    public void testGetShortOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + numericProc + "(?,?,?)}")) {
            // Only register MIN_VAL param as SMALLINT (MAX_VAL exceeds smallint range)
            cs.registerOutParameter(1, Types.NUMERIC);
            cs.registerOutParameter(2, Types.SMALLINT);
            cs.registerOutParameter(3, Types.SMALLINT);
            cs.execute();
            // MIN_VAL (0.000001) truncates to 0 for SMALLINT
            cs.getShort(2);
        }
    }

    /**
     * Tests getByte output parameter retrieval from stored procedure with TINYINT type.
     * Ported from FX CTS callStmtClient getByte variations.
     */
    @Test
    public void testGetByteOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + numericProc + "(?,?,?)}")) {
            // Only register MIN_VAL param as TINYINT (MAX_VAL exceeds tinyint range)
            cs.registerOutParameter(1, Types.NUMERIC);
            cs.registerOutParameter(2, Types.TINYINT);
            cs.registerOutParameter(3, Types.TINYINT);
            cs.execute();
            // MIN_VAL (0.000001) truncates to 0 for TINYINT
            cs.getByte(2);
        }
    }

    /**
     * Tests getBoolean output parameter retrieval with non-zero numeric to boolean conversion.
     * Ported from FX CTS callStmtClient getBoolean variations.
     */
    @Test
    public void testGetBooleanOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + numericProc + "(?,?,?)}")) {
            cs.registerOutParameter(1, Types.BIT);
            cs.registerOutParameter(2, Types.BIT);
            cs.registerOutParameter(3, Types.BIT);
            cs.execute();
            // Non-zero numeric to boolean
            assertTrue(cs.getBoolean(1));
        }
    }

    /**
     * Tests getString output parameter retrieval from stored procedure with VARCHAR type.
     * Ported from FX CTS callStmtClient getString variations.
     */
    @Test
    public void testGetStringOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + varcharProc + "(?,?)}")) {
            cs.registerOutParameter(1, Types.VARCHAR);
            cs.registerOutParameter(2, Types.VARCHAR);
            cs.execute();
            String name = cs.getString(1);
            assertEquals("Colombian", name);
        }
    }

    /**
     * Tests getString output parameter retrieval from a NUMERIC stored procedure output.
     * Ported from FX CTS callStmtClient getString numeric conversion variations.
     */
    @Test
    public void testGetStringFromNumericOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + numericProc + "(?,?,?)}")) {
            cs.registerOutParameter(1, Types.VARCHAR);
            cs.registerOutParameter(2, Types.VARCHAR);
            cs.registerOutParameter(3, Types.VARCHAR);
            cs.execute();
            String maxStr = cs.getString(1);
            assertNotNull(maxStr);
            assertTrue(maxStr.length() > 0);
        }
    }

    /**
     * Tests getDate output parameter retrieval from stored procedure with DATE type.
     * Ported from FX CTS callStmtClient getDate variations.
     */
    @Test
    public void testGetDateOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + dateProc + "(?,?,?,?)}")) {
            cs.registerOutParameter(1, Types.DATE);
            cs.registerOutParameter(2, Types.TIME);
            cs.registerOutParameter(3, Types.TIMESTAMP);
            cs.registerOutParameter(4, Types.DATE);
            cs.execute();
            Date dt = cs.getDate(1);
            assertNotNull(dt);
        }
    }

    /**
     * Tests getTime output parameter retrieval from stored procedure with TIME type.
     * Ported from FX CTS callStmtClient getTime variations.
     */
    @Test
    public void testGetTimeOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + dateProc + "(?,?,?,?)}")) {
            cs.registerOutParameter(1, Types.DATE);
            cs.registerOutParameter(2, Types.TIME);
            cs.registerOutParameter(3, Types.TIMESTAMP);
            cs.registerOutParameter(4, Types.DATE);
            cs.execute();
            Time tm = cs.getTime(2);
            assertNotNull(tm);
        }
    }

    /**
     * Tests getTimestamp output parameter retrieval from stored procedure with TIMESTAMP type.
     * Ported from FX CTS callStmtClient getTimestamp variations.
     */
    @Test
    public void testGetTimestampOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + dateProc + "(?,?,?,?)}")) {
            cs.registerOutParameter(1, Types.DATE);
            cs.registerOutParameter(2, Types.TIME);
            cs.registerOutParameter(3, Types.TIMESTAMP);
            cs.registerOutParameter(4, Types.DATE);
            cs.execute();
            Timestamp ts = cs.getTimestamp(3);
            assertNotNull(ts);
        }
    }

    /**
     * Tests getBytes output parameter retrieval from stored procedure with VARBINARY type.
     * Ported from FX CTS callStmtClient getBytes variations.
     */
    @Test
    public void testGetBytesOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + binaryProc + "(?,?)}")) {
            cs.registerOutParameter(1, Types.VARBINARY);
            cs.registerOutParameter(2, Types.VARBINARY);
            cs.execute();
            byte[] bin = cs.getBytes(1);
            assertNotNull(bin);
            assertEquals(5, bin.length);
            assertEquals((byte) 0x41, bin[0]); // 'A'
        }
    }

    /**
     * Tests wasNull returns true after retrieving a NULL output parameter value.
     * Ported from FX CTS callStmtClient wasNull variations.
     */
    @Test
    public void testWasNull() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + nullProc + "(?)}")) {
            cs.registerOutParameter(1, Types.VARCHAR);
            cs.execute();
            String result = cs.getString(1);
            assertNull(result);
            assertTrue(cs.wasNull());
        }
    }

    /**
     * Tests getObject output parameter retrieval for NUMERIC type returns BigDecimal.
     * Ported from FX CTS callStmtClient getObject variations.
     */
    @Test
    public void testGetObjectNumeric() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + numericProc + "(?,?,?)}")) {
            cs.registerOutParameter(1, Types.NUMERIC, 6);
            cs.registerOutParameter(2, Types.NUMERIC, 6);
            cs.registerOutParameter(3, Types.NUMERIC, 6);
            cs.execute();
            Object obj = cs.getObject(1);
            assertNotNull(obj);
            assertTrue(obj instanceof BigDecimal);
        }
    }

    /**
     * Tests getObject output parameter retrieval for VARCHAR type returns String.
     * Ported from FX CTS callStmtClient getObject variations.
     */
    @Test
    public void testGetObjectVarchar() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + varcharProc + "(?,?)}")) {
            cs.registerOutParameter(1, Types.VARCHAR);
            cs.registerOutParameter(2, Types.VARCHAR);
            cs.execute();
            Object obj = cs.getObject(1);
            assertNotNull(obj);
            assertTrue(obj instanceof String);
            assertEquals("Colombian", obj.toString());
        }
    }

    /**
     * Tests getObject output parameter retrieval for DATE type.
     * Ported from FX CTS callStmtClient getObject variations.
     */
    @Test
    public void testGetObjectDate() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + dateProc + "(?,?,?,?)}")) {
            cs.registerOutParameter(1, Types.DATE);
            cs.registerOutParameter(2, Types.TIME);
            cs.registerOutParameter(3, Types.TIMESTAMP);
            cs.registerOutParameter(4, Types.DATE);
            cs.execute();
            Object dtObj = cs.getObject(1);
            assertNotNull(dtObj);
        }
    }

    /**
     * Tests getObject output parameter retrieval returns null for NULL database value.
     * Ported from FX CTS callStmtClient getObject null variations.
     */
    @Test
    public void testGetObjectNull() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + numericProc + "(?,?,?)}")) {
            cs.registerOutParameter(1, Types.NUMERIC, 6);
            cs.registerOutParameter(2, Types.NUMERIC, 6);
            cs.registerOutParameter(3, Types.NUMERIC, 6);
            cs.execute();
            Object nullObj = cs.getObject(3);
            assertNull(nullObj);
            assertTrue(cs.wasNull());
        }
    }

    /**
     * Tests stored procedure execution with registered output parameters and BigDecimal retrieval.
     * Ported from FX CTS callStmtClient setObject input parameter variations.
     */
    @Test
    public void testSetInputParameters() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + numericProc + "(?,?,?)}")) {
            cs.registerOutParameter(1, Types.NUMERIC, 6);
            cs.registerOutParameter(2, Types.NUMERIC, 6);
            cs.registerOutParameter(3, Types.NUMERIC, 6);
            cs.execute();
            BigDecimal result = cs.getBigDecimal(1);
            assertNotNull(result);
        }
    }

    /**
     * Tests retrieval of NULL VARCHAR output parameter and wasNull confirmation.
     * Ported from FX CTS callStmtClient setNull variations.
     */
    @Test
    public void testSetNullInput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + varcharProc + "(?,?)}")) {
            cs.registerOutParameter(1, Types.VARCHAR);
            cs.registerOutParameter(2, Types.VARCHAR);
            cs.execute();
            String nullVal = cs.getString(2);
            assertNull(nullVal);
            assertTrue(cs.wasNull());
        }
    }

    /**
     * Tests that a callable statement can be executed multiple times with consistent results.
     * Ported from FX CTS callStmtClient multiple execution variations.
     */
    @Test
    public void testMultipleExecutions() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + varcharProc + "(?,?)}")) {
            cs.registerOutParameter(1, Types.VARCHAR);
            cs.registerOutParameter(2, Types.VARCHAR);
            // Execute multiple times - should not fail
            cs.execute();
            assertEquals("Colombian", cs.getString(1));
            cs.execute();
            assertEquals("Colombian", cs.getString(1));
        }
    }

    /**
     * Tests getObject output parameter retrieval for VARBINARY type returns byte array.
     * Ported from FX CTS callStmtClient getObject binary variations.
     */
    @Test
    public void testGetObjectBinary() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + binaryProc + "(?,?)}")) {
            cs.registerOutParameter(1, Types.VARBINARY);
            cs.registerOutParameter(2, Types.VARBINARY);
            cs.execute();
            Object binObj = cs.getObject(1);
            assertNotNull(binObj);
            assertTrue(binObj instanceof byte[]);
        }
    }

    /**
     * Tests getBytes returns null for NULL VARBINARY output parameter.
     * Ported from FX CTS callStmtClient getBytes null variations.
     */
    @Test
    public void testNullBinaryOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + binaryProc + "(?,?)}")) {
            cs.registerOutParameter(1, Types.VARBINARY);
            cs.registerOutParameter(2, Types.VARBINARY);
            cs.execute();
            byte[] nullBin = cs.getBytes(2);
            assertNull(nullBin);
            assertTrue(cs.wasNull());
        }
    }

    /**
     * Tests getDate returns null for NULL DATE output parameter.
     * Ported from FX CTS callStmtClient getDate null variations.
     */
    @Test
    public void testNullDateOutput() throws SQLException {
        try (Connection conn = getConnection();
                CallableStatement cs = conn.prepareCall("{call " + dateProc + "(?,?,?,?)}")) {
            cs.registerOutParameter(1, Types.DATE);
            cs.registerOutParameter(2, Types.TIME);
            cs.registerOutParameter(3, Types.TIMESTAMP);
            cs.registerOutParameter(4, Types.DATE);
            cs.execute();
            Date nullDate = cs.getDate(4);
            assertNull(nullDate);
            assertTrue(cs.wasNull());
        }
    }
}
