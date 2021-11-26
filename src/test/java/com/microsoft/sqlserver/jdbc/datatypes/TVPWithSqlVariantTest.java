/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;
import com.microsoft.sqlserver.testframework.sqlType.SqlDate;


@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class TVPWithSqlVariantTest extends AbstractTest {

    private static SQLServerConnection conn = null;
    static SQLServerStatement stmt = null;
    static SQLServerDataTable tvp = null;

    private static String tvpName = RandomUtil.getIdentifier("numericTVP");
    private static String destTable = RandomUtil.getIdentifier("destTvpSqlVariantTable");
    private static String procedureName = RandomUtil.getIdentifier("procedureThatCallsTVP");

    /**
     * Test a previous failure regarding to numeric precision. Issue #211
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testInt() throws SQLException {
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        tvp.addRow(12);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }

        try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
            while (rs.next()) {
                assertEquals(rs.getInt(1), 12);
                assertEquals(rs.getString(1), "" + 12);
                assertEquals(rs.getObject(1), 12);
            }
        }
    }

    /**
     * Test with date value
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testDate() throws SQLException {
        SqlDate sqlDate = new SqlDate();
        Date date = (Date) sqlDate.createdata();
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        tvp.addRow(date);
        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }

        try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
            while (rs.next()) {
                assertEquals(rs.getString(1), "" + date); // TODO: GetDate has issues
            }
        }
    }

    /**
     * Test with money value
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testMoney() throws SQLException {
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        String[] numeric = createNumericValues();
        tvp.addRow(new BigDecimal(numeric[14]));
        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }

        try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
            while (rs.next()) {
                assertEquals(rs.getMoney(1), new BigDecimal(numeric[14]));
            }
        }
    }

    /**
     * Test with small int value
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testSmallInt() throws SQLException {
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        String[] numeric = createNumericValues();
        tvp.addRow(Short.valueOf(numeric[2]));
        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();

        }

        try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
            while (rs.next()) {
                assertEquals("" + rs.getInt(1), numeric[2]);
                // System.out.println(rs.getShort(1)); //does not work says cannot cast integer to short cause it is
                // written
                // as int
            }
        }
    }

    /**
     * Test with bigint value
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testBigInt() throws SQLException {
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        String[] numeric = createNumericValues();
        tvp.addRow(Long.parseLong(numeric[4]));

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }
        try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
            while (rs.next()) {
                assertEquals(rs.getLong(1), Long.parseLong(numeric[4]));
            }
        }
    }

    /**
     * Test with boolean value
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testBoolean() throws SQLException {
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        String[] numeric = createNumericValues();
        tvp.addRow(Boolean.parseBoolean(numeric[0]));
        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }
        try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
            while (rs.next()) {
                assertEquals(rs.getBoolean(1), Boolean.parseBoolean(numeric[0]));
            }
        }
    }

    /**
     * Test with float value
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testFloat() throws SQLException {
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        String[] numeric = createNumericValues();
        tvp.addRow(Float.parseFloat(numeric[1]));
        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }
        try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
            while (rs.next()) {
                assertEquals(rs.getFloat(1), Float.parseFloat(numeric[1]));
            }
        }
    }

    /**
     * Test with nvarchar
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testNvarChar() throws SQLException {
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        String colValue = "س";
        tvp.addRow(colValue);
        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }
        try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
            while (rs.next()) {
                assertEquals(rs.getString(1), colValue);
            }
        }
    }

    /**
     * Test with varchar8000
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testVarChar8000() throws SQLException {
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8000; i++) {
            buffer.append("a");
        }
        String value = buffer.toString();
        tvp.addRow(value);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }
        try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
            while (rs.next()) {
                assertEquals(rs.getString(1), value);
            }
        }
    }

    /**
     * Check that we throw proper error message when inserting more than 8000
     * 
     * @throws SQLException
     */
    @Test
    public void testLongVarChar() throws SQLException {
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);

        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 8001; i++) {
            buffer.append("a");
        }
        String value = buffer.toString();
        tvp.addRow(value);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            try {
                pstmt.execute();
            } catch (SQLException e) {
                assertTrue(e.getMessage()
                        .contains("SQL_VARIANT does not support string values of length greater than 8000."));
            } catch (Exception e) {
                // Test should have failed! mistakenly inserted string value of more than 8000 in sql-variant
                fail(TestResource.getResource("R_unexpectedException"));
            }
        }
    }

    /**
     * Test with datetime
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testDateTime() throws SQLException {
        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2007-09-23 10:10:10.0");
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        tvp.addRow(timestamp);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }
        try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
            while (rs.next()) {
                assertEquals(rs.getString(1), "" + timestamp);
                // System.out.println(rs.getDateTime(1));// TODO does not work
            }
        }
    }

    /**
     * Test with null value
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     *         https://msdn.microsoft.com/en-ca/library/dd303302.aspx?f=255&MSPPError=-2147217396 Data types cannot be
     *         NULL when inside a sql_variant
     */
    @Test
    public void testNull() throws SQLException {
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        try {
            tvp.addRow((Date) null);
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith(TestUtils.R_BUNDLE.getString("R_invalidValueForTVPWithSQLVariant")));
        }

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(destTable) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }
        try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
            while (rs.next()) {}
        }
    }

    /**
     * Test with stored procedure
     * 
     * @throws SQLException
     * @throws SQLTimeoutException
     */
    @Test
    public void testIntStoredProcedure() throws SQLException {
        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2007-09-23 10:10:10.0");
        final String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        tvp.addRow(timestamp);
        try (SQLServerCallableStatement cstatement = (SQLServerCallableStatement) conn.prepareCall(sql)) {
            cstatement.setStructured(1, tvpName, tvp);
            cstatement.execute();
            try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
                while (rs.next()) {}
            }
        }
    }

    /**
     * Test for allowing duplicate columns
     * 
     * @throws SQLException
     */
    @Test
    public void testDuplicateColumn() throws SQLException {
        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", microsoft.sql.Types.SQL_VARIANT);
        tvp.addColumnMetadata("c2", microsoft.sql.Types.SQL_VARIANT);
        try {
            tvp.addColumnMetadata("c2", microsoft.sql.Types.SQL_VARIANT);
        } catch (SQLException e) {
            assertEquals(e.getMessage(), "A column name c2 already belongs to this SQLServerDataTable.");
        }
    }

    private static String[] createNumericValues() {
        Boolean C1_BIT;
        Short C2_TINYINT;
        Short C3_SMALLINT;
        Integer C4_INT;
        Long C5_BIGINT;
        Double C6_FLOAT;
        Double C7_FLOAT;
        Float C8_REAL;
        BigDecimal C9_DECIMAL;
        BigDecimal C10_DECIMAL;
        BigDecimal C11_NUMERIC;

        boolean nullable = false;
        RandomData.returnNull = nullable;
        C1_BIT = RandomData.generateBoolean(nullable);
        C2_TINYINT = RandomData.generateTinyint(nullable);
        C3_SMALLINT = RandomData.generateSmallint(nullable);
        C4_INT = RandomData.generateInt(nullable);
        C5_BIGINT = RandomData.generateLong(nullable);
        C6_FLOAT = RandomData.generateFloat(24, nullable);
        C7_FLOAT = RandomData.generateFloat(53, nullable);
        C8_REAL = RandomData.generateReal(nullable);
        C9_DECIMAL = RandomData.generateDecimalNumeric(18, 0, nullable);
        C10_DECIMAL = RandomData.generateDecimalNumeric(10, 5, nullable);
        C11_NUMERIC = RandomData.generateDecimalNumeric(18, 0, nullable);
        BigDecimal C12_NUMERIC = RandomData.generateDecimalNumeric(8, 2, nullable);
        BigDecimal C13_smallMoney = RandomData.generateSmallMoney(nullable);
        BigDecimal C14_money = RandomData.generateMoney(nullable);
        BigDecimal C15_decimal = RandomData.generateDecimalNumeric(28, 4, nullable);
        BigDecimal C16_numeric = RandomData.generateDecimalNumeric(28, 4, nullable);

        String[] numericValues = {"" + C1_BIT, "" + C2_TINYINT, "" + C3_SMALLINT, "" + C4_INT, "" + C5_BIGINT,
                "" + C6_FLOAT, "" + C7_FLOAT, "" + C8_REAL, "" + C9_DECIMAL, "" + C10_DECIMAL, "" + C11_NUMERIC,
                "" + C12_NUMERIC, "" + C13_smallMoney, "" + C14_money, "" + C15_decimal, "" + C16_numeric};

        if (RandomData.returnZero && !RandomData.returnNull) {
            C10_DECIMAL = new BigDecimal(0);
            C12_NUMERIC = new BigDecimal(0);
            C13_smallMoney = new BigDecimal(0);
            C14_money = new BigDecimal(0);
            C15_decimal = new BigDecimal(0);
            C16_numeric = new BigDecimal(0);
        }
        return numericValues;
    }

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    @BeforeEach
    public void testSetup() throws SQLException {
        conn = (SQLServerConnection) PrepUtil.getConnection(connectionString + ";sendStringParametersAsUnicode=true;");
        stmt = (SQLServerStatement) conn.createStatement();

        TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procedureName), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(destTable), stmt);
        dropTVPS();

        createTVPS();
        createTables();
        createPreocedure();
    }

    private static void dropTVPS() throws SQLException {
        stmt.executeUpdate("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '"
                + TestUtils.escapeSingleQuotes(tvpName) + "') " + " drop type "
                + AbstractSQLGenerator.escapeIdentifier(tvpName));
    }

    private static void createPreocedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName) + " @InputData "
                + AbstractSQLGenerator.escapeIdentifier(tvpName) + " READONLY " + " AS " + " BEGIN " + " INSERT INTO "
                + AbstractSQLGenerator.escapeIdentifier(destTable) + " SELECT * FROM @InputData" + " END";

        stmt.execute(sql);
    }

    private void createTables() throws SQLException {
        String sql = "create table " + AbstractSQLGenerator.escapeIdentifier(destTable) + " (c1 sql_variant null);";
        stmt.execute(sql);
    }

    private void createTVPS() throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                + " as table (c1 sql_variant null)";
        stmt.executeUpdate(TVPCreateCmd);
    }

    @AfterEach
    public void terminateVariation() throws SQLException {
        TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procedureName), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(destTable), stmt);
        dropTVPS();
        if (null != stmt) {
            stmt.close();
        }
        if (null != conn) {
            conn.close();
        }
    }
}
