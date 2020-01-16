/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedList;

import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

import microsoft.sql.DateTimeOffset;


/**
 * Test cases related to SQLServerCallableStatement.
 *
 */
@RunWith(Parameterized.class)
@Tag(Constants.xSQLv12)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
public class CallableStatementTest extends AESetup {

    public CallableStatementTest(String serverName, String url, String protocol) throws Exception {
        super(serverName, url, protocol);
    }

    private static String multiStatementsProcedure = RandomUtil.getIdentifier("multiStatementsProcedure");
    private static String inputProcedure = RandomUtil.getIdentifier("inputProcedure");
    private static String inputProcedure2 = RandomUtil.getIdentifier("inputProcedure2");
    private static String outputProcedure = RandomUtil.getIdentifier("outputProcedure");
    private static String outputProcedure2 = RandomUtil.getIdentifier("outputProcedure2");
    private static String outputProcedure3 = RandomUtil.getIdentifier("outputProcedure3");
    private static String outputProcedure4 = RandomUtil.getIdentifier("outputProcedure4");
    private static String outputProcedureChar = RandomUtil.getIdentifier("outputProcedureChar");
    private static String outputProcedureNumeric = RandomUtil.getIdentifier("outputProcedureNumeric");
    private static String outputProcedureBinary = RandomUtil.getIdentifier("outputProcedureBinary");
    private static String outputProcedureDate = RandomUtil.getIdentifier("outputProcedureDate");
    private static String outputProcedureDateScale = RandomUtil.getIdentifier("outputProcedureDateScale");
    private static String outputProcedureBatch = RandomUtil.getIdentifier("outputProcedureBatch");
    private static String inoutProcedure = RandomUtil.getIdentifier("inoutProcedure");
    private static String mixedProcedure = RandomUtil.getIdentifier("mixedProcedure");
    private static String mixedProcedure2 = RandomUtil.getIdentifier("mixedProcedure2");
    private static String mixedProcedure3 = RandomUtil.getIdentifier("mixedProcedure3");
    private static String mixedProcedureNumericPrcisionScale = RandomUtil
            .getIdentifier("mixedProcedureNumericPrcisionScale");

    private static String table1 = RandomUtil.getIdentifier("StoredProcedure_table1");
    private static String table2 = RandomUtil.getIdentifier("StoredProcedure_table2");
    private static String table3 = RandomUtil.getIdentifier("StoredProcedure_table3");
    private static String table4 = RandomUtil.getIdentifier("StoredProcedure_table4");
    private static String table5 = RandomUtil.getIdentifier("StoredProcedure_table5");
    private static String table6 = RandomUtil.getIdentifier("StoredProcedure_table6");

    private static String[] numericValues;
    private static LinkedList<byte[]> byteValues;
    private static String[] charValues;
    private static LinkedList<Object> dateValues;
    private static boolean nullable = false;

    static String SP_table1[][] = {{"Char", "char(20) COLLATE Latin1_General_BIN2"},
            {"Varchar", "varchar(50) COLLATE Latin1_General_BIN2"},};
    static String SP_table2[][] = {{"Char", "char(20) COLLATE Latin1_General_BIN2"},
            {"Varchar", "varchar(50) COLLATE Latin1_General_BIN2"},};
    static String SP_table3[][] = {{"Bit", "bit"}, {"Tinyint", "tinyint"}, {"Smallint", "smallint"}, {"Int", "int"},
            {"BigInt", "bigint"}, {"FloatDefault", "float"}, {"Float", "float(30)"}, {"Real", "real"},
            {"DecimalDefault", "decimal(18,0)"}, {"Decimal", "decimal(10,5)"}, {"NumericDefault", "numeric(18,0)"},
            {"Numeric", "numeric(8,2)"}, {"Int2", "int"}, {"SmallMoney", "smallmoney"}, {"Money", "money"},
            {"Decimal2", "decimal(28,4)"}, {"Numeric2", "numeric(28,4)"},};
    static String SP_table4[][] = {{"Int", "int"},};

    /**
     * Initialize the tables for this class. This method will execute AFTER the parent class (AESetup) finishes
     * initializing.
     * 
     * @throws SQLException
     */
    @BeforeAll
    public static void initCallableStatementTest() throws Exception {
        dropTables();

        numericValues = createNumericValues(nullable);
        byteValues = createBinaryValues(nullable);
        dateValues = createTemporalTypesCallableStatement(nullable);
        charValues = createCharValues(nullable);

        createTables(cekJks);
        populateTable3();
        populateTable4();

        createTable(CHAR_TABLE_AE, cekJks, charTable);
        createTable(NUMERIC_TABLE_AE, cekJks, numericTable);
        createTable(BINARY_TABLE_AE, cekJks, binaryTable);

        createDateTableCallableStatement(cekJks);
        populateCharNormalCase(charValues);
        populateNumericSetObject(numericValues);
        populateBinaryNormalCase(byteValues);
        populateDateNormalCase();

        createTable(SCALE_DATE_TABLE_AE, cekJks, dateScaleTable);
        populateDateScaleNormalCase(dateValues);
    }

    @AfterAll
    public static void dropAll() throws Exception {
        dropTables();
        dropProcedures();
    }

    @Test
    public void testMultiInsertionSelection() throws SQLException {
        createMultiInsertionSelection();
        MultiInsertionSelection();
    }

    @Test
    public void testInputProcedureNumeric() throws SQLException {
        createInputProcedure();
        testInputProcedure(
                "{call " + AbstractSQLGenerator.escapeIdentifier(inputProcedure) + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}",
                numericValues);
    }

    @Test
    public void testInputProcedureChar() throws SQLException {
        createInputProcedure2();
        testInputProcedure2("{call " + AbstractSQLGenerator.escapeIdentifier(inputProcedure2) + "(?,?,?,?,?,?,?,?)}");
    }

    @Test
    public void testEncryptedOutputNumericParams() throws SQLException {
        createOutputProcedure();
        testOutputProcedureRandomOrder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedure) + "(?,?,?,?,?,?,?)}", numericValues);
        testOutputProcedureInorder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedure) + "(?,?,?,?,?,?,?)}", numericValues);
        testOutputProcedureReverseOrder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedure) + "(?,?,?,?,?,?,?)}", numericValues);
        testOutputProcedureRandomOrder(
                "exec " + AbstractSQLGenerator.escapeIdentifier(outputProcedure) + " ?,?,?,?,?,?,?", numericValues);
    }

    @Test
    public void testUnencryptedAndEncryptedNumericOutputParams() throws SQLException {
        createOutputProcedure2();
        testOutputProcedure2RandomOrder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedure2) + "(?,?,?,?,?,?,?,?,?,?)}",
                numericValues);
        testOutputProcedure2Inorder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedure2) + "(?,?,?,?,?,?,?,?,?,?)}",
                numericValues);
        testOutputProcedure2ReverseOrder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedure2) + "(?,?,?,?,?,?,?,?,?,?)}",
                numericValues);
    }

    @Test
    public void testEncryptedOutputParamsFromDifferentTables() throws SQLException {
        createOutputProcedure3();
        testOutputProcedure3RandomOrder("{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedure3) + "(?,?)}");
        testOutputProcedure3Inorder("{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedure3) + "(?,?)}");
        testOutputProcedure3ReverseOrder("{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedure3) + "(?,?)}");
    }

    @Test
    public void testInOutProcedure() throws SQLException {
        createInOutProcedure();
        testInOutProcedure("{call " + AbstractSQLGenerator.escapeIdentifier(inoutProcedure) + "(?)}");
        testInOutProcedure("exec " + AbstractSQLGenerator.escapeIdentifier(inoutProcedure) + " ?");
    }

    @Test
    public void testMixedProcedure() throws SQLException {
        createMixedProcedure();
        testMixedProcedure("{ ? = call " + AbstractSQLGenerator.escapeIdentifier(mixedProcedure) + "(?,?,?)}");
    }

    @Test
    public void testUnencryptedAndEncryptedIOParams() throws SQLException {
        // unencrypted input and output parameter
        // encrypted input and output parameter
        createMixedProcedure2();
        testMixedProcedure2RandomOrder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(mixedProcedure2) + "(?,?,?,?)}");
        testMixedProcedure2Inorder("{call " + AbstractSQLGenerator.escapeIdentifier(mixedProcedure2) + "(?,?,?,?)}");
    }

    @Test
    public void testUnencryptedIOParams() throws SQLException {
        createMixedProcedure3();
        testMixedProcedure3RandomOrder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(mixedProcedure3) + "(?,?,?,?)}");
        testMixedProcedure3Inorder("{call " + AbstractSQLGenerator.escapeIdentifier(mixedProcedure3) + "(?,?,?,?)}");
        testMixedProcedure3ReverseOrder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(mixedProcedure3) + "(?,?,?,?)}");
    }

    @Test
    public void testVariousIOParams() throws SQLException {
        createMixedProcedureNumericPrcisionScale();
        testMixedProcedureNumericPrcisionScaleInorder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(mixedProcedureNumericPrcisionScale) + "(?,?,?,?)}");
        testMixedProcedureNumericPrcisionScaleParameterName(
                "{call " + AbstractSQLGenerator.escapeIdentifier(mixedProcedureNumericPrcisionScale) + "(?,?,?,?)}");
    }

    @Test
    public void testOutputProcedureChar() throws SQLException {
        createOutputProcedureChar();
        testOutputProcedureCharInorder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureChar) + "(?,?,?,?,?,?,?,?,?)}");
        testOutputProcedureCharInorderObject(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureChar) + "(?,?,?,?,?,?,?,?,?)}");
    }

    @Test
    public void testOutputProcedureNumeric() throws SQLException {
        createOutputProcedureNumeric();
        testOutputProcedureNumericInorder("{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureNumeric)
                + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
        testcoerctionsOutputProcedureNumericInorder("{call "
                + AbstractSQLGenerator.escapeIdentifier(outputProcedureNumeric) + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
    }

    @Test
    public void testOutputProcedureBinary() throws SQLException {
        createOutputProcedureBinary();
        testOutputProcedureBinaryInorder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureBinary) + "(?,?,?,?,?)}");
        testOutputProcedureBinaryInorderObject(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureBinary) + "(?,?,?,?,?)}");
        testOutputProcedureBinaryInorderString(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureBinary) + "(?,?,?,?,?)}");
    }

    @Test
    public void testOutputProcedureDate() throws SQLException {
        createOutputProcedureDate();
        testOutputProcedureDateInorder("{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureDate)
                + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
        testOutputProcedureDateInorderObject("{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureDate)
                + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
    }

    @Test
    public void testMixedProcedureDateScale() throws SQLException {
        createMixedProcedureDateScale();
        testMixedProcedureDateScaleInorder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureDateScale) + "(?,?,?,?,?,?)}");
        testMixedProcedureDateScaleWithParameterName(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureDateScale) + "(?,?,?,?,?,?)}");
    }

    @Test
    public void testOutputProcedureBatch() throws SQLException {
        createOutputProcedureBatch();
        testOutputProcedureBatchInorder(
                "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureBatch) + "(?,?,?,?)}");
    }

    @Test
    public void testOutputProcedure4() throws SQLException {
        createOutputProcedure4();
    }

    private static void dropProcedures() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(multiStatementsProcedure), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputProcedure), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputProcedure2), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedure), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedure2), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedure3), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedure4), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedureChar), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedureNumeric), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedureBinary), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedureDate), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedureDateScale), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedureBatch), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inoutProcedure), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(mixedProcedure), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(mixedProcedure2), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(mixedProcedure3), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(mixedProcedureNumericPrcisionScale),
                    stmt);
        }
    }

    private static void dropTables() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table1), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table2), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table3), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table4), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(CHAR_TABLE_AE), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(BINARY_TABLE_AE), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(DATE_TABLE_AE), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table5), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table6), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(SCALE_DATE_TABLE_AE), stmt);
        }
    }

    private static void createTables(String cekName) throws SQLException {
        createTable(table1, cekJks, SP_table1);
        createTable(table2, cekJks, SP_table2);
        createTable(table3, cekJks, SP_table3);
        createTable(table4, cekJks, SP_table4);

        String sql = "create table " + AbstractSQLGenerator.escapeIdentifier(table5) + " ("
                + "c1 int ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekJks + ") NULL,"
                + "c2 smallint ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekJks + ") NULL,"
                + "c3 bigint ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekJks + ") NULL," + ");";

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                Statement stmt = con.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        sql = "create table " + AbstractSQLGenerator.escapeIdentifier(table6) + " ("
                + "c1 int ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "c2 smallint ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "c3 bigint ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL," + ");";

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                Statement stmt = con.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private static void populateTable4() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(table4) + " values( " + "?,?,?" + ")";

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                PreparedStatement pstmt = TestUtils.getPreparedStmt(con, sql, stmtColEncSetting)) {

            // bit
            for (int i = 1; i <= 3; i++) {
                pstmt.setInt(i, Integer.parseInt(numericValues[3]));
            }

            pstmt.execute();
        }
    }

    private static void populateTable3() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(table3) + " values( " + "?,?,?," + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // bit
            for (int i = 1; i <= 3; i++) {
                if (numericValues[0].equalsIgnoreCase(Boolean.TRUE.toString())) {
                    pstmt.setBoolean(i, true);
                } else {
                    pstmt.setBoolean(i, false);
                }
            }

            // tinyint
            for (int i = 4; i <= 6; i++) {
                pstmt.setShort(i, Short.valueOf(numericValues[1]));
            }

            // smallint
            for (int i = 7; i <= 9; i++) {
                pstmt.setShort(i, Short.parseShort(numericValues[2]));
            }

            // int
            for (int i = 10; i <= 12; i++) {
                pstmt.setInt(i, Integer.parseInt(numericValues[3]));
            }

            // bigint
            for (int i = 13; i <= 15; i++) {
                pstmt.setLong(i, Long.parseLong(numericValues[4]));
            }

            // float default
            for (int i = 16; i <= 18; i++) {
                pstmt.setDouble(i, Double.parseDouble(numericValues[5]));
            }

            // float(30)
            for (int i = 19; i <= 21; i++) {
                pstmt.setDouble(i, Double.parseDouble(numericValues[6]));
            }

            // real
            for (int i = 22; i <= 24; i++) {
                pstmt.setFloat(i, Float.parseFloat(numericValues[7]));
            }

            // decimal default
            for (int i = 25; i <= 27; i++) {
                if (numericValues[8].equalsIgnoreCase("0"))
                    pstmt.setBigDecimal(i, new BigDecimal(numericValues[8]), 18, 0);
                else
                    pstmt.setBigDecimal(i, new BigDecimal(numericValues[8]));
            }

            // decimal(10,5)
            for (int i = 28; i <= 30; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(numericValues[9]), 10, 5);
            }

            // numeric
            for (int i = 31; i <= 33; i++) {
                if (numericValues[10].equalsIgnoreCase("0"))
                    pstmt.setBigDecimal(i, new BigDecimal(numericValues[10]), 18, 0);
                else
                    pstmt.setBigDecimal(i, new BigDecimal(numericValues[10]));
            }

            // numeric(8,2)
            for (int i = 34; i <= 36; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(numericValues[11]), 8, 2);
            }

            // int2
            for (int i = 37; i <= 39; i++) {
                pstmt.setInt(i, Integer.parseInt(numericValues[3]));
            }
            // smallmoney
            for (int i = 40; i <= 42; i++) {
                pstmt.setSmallMoney(i, new BigDecimal(numericValues[12]));
            }

            // money
            for (int i = 43; i <= 45; i++) {
                pstmt.setMoney(i, new BigDecimal(numericValues[13]));
            }

            // decimal(28,4)
            for (int i = 46; i <= 48; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(numericValues[14]), 28, 4);
            }

            // numeric(28,4)
            for (int i = 49; i <= 51; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(numericValues[15]), 28, 4);
            }

            pstmt.execute();
        }
    }

    private void createMultiInsertionSelection() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(multiStatementsProcedure)
                + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)" + " DROP PROCEDURE "
                + AbstractSQLGenerator.escapeIdentifier(multiStatementsProcedure);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(multiStatementsProcedure)
                    + " (@p0 char(20) = null, @p1 char(20) = null, @p2 char(20) = null, "
                    + "@p3 varchar(50) = null, @p4 varchar(50) = null, @p5 varchar(50) = null)" + " AS"
                    + " INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(table1)
                    + " values (@p0,@p1,@p2,@p3,@p4,@p5)" + " INSERT INTO "
                    + AbstractSQLGenerator.escapeIdentifier(table2) + " values (@p0,@p1,@p2,@p3,@p4,@p5)"
                    + " SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(table1) + " SELECT * FROM "
                    + AbstractSQLGenerator.escapeIdentifier(table2);
            stmt.execute(sql);
        }
    }

    private void MultiInsertionSelection() throws SQLException {

        String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(multiStatementsProcedure) + " (?,?,?,?,?,?)}";
        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            // char, varchar
            for (int i = 1; i <= 3; i++) {
                callableStatement.setString(i, charValues[0]);
            }

            for (int i = 4; i <= 6; i++) {
                callableStatement.setString(i, charValues[1]);
            }

            boolean results = callableStatement.execute();

            // skip update count which is given by insertion
            while (false == results && (-1) != callableStatement.getUpdateCount()) {
                results = callableStatement.getMoreResults();
            }

            while (results) {
                try (ResultSet rs = callableStatement.getResultSet()) {
                    int numberOfColumns = rs.getMetaData().getColumnCount();

                    while (rs.next()) {
                        testGetString(rs, numberOfColumns);
                    }
                }
                results = callableStatement.getMoreResults();
            }
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void testGetString(ResultSet rs, int numberOfColumns) throws SQLException {
        for (int i = 1; i <= numberOfColumns; i = i + 3) {

            String stringValue1 = "" + rs.getString(i);
            String stringValue2 = "" + rs.getString(i + 1);
            String stringValue3 = "" + rs.getString(i + 2);

            assertTrue(stringValue1.equalsIgnoreCase(stringValue2) && stringValue2.equalsIgnoreCase(stringValue3),
                    "Decryption failed with getString(): " + stringValue1 + ", " + stringValue2 + ", " + stringValue3
                            + ".\n");

        }
    }

    private void createInputProcedure() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(inputProcedure) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputProcedure);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputProcedure)
                    + " @p0 int, @p1 decimal(18, 0), "
                    + "@p2 float, @p3 real, @p4 numeric(18, 0), @p5 smallmoney, @p6 money,"
                    + "@p7 bit, @p8 smallint, @p9 bigint, @p10 float(30), @p11 decimal(10,5), @p12 numeric(8,2), "
                    + "@p13 decimal(28,4), @p14 numeric(28,4)  " + " AS" + " SELECT top 1 RandomizedInt FROM "
                    + AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE)
                    + " where DeterministicInt=@p0 and DeterministicDecimalDefault=@p1 and "
                    + " DeterministicFloatDefault=@p2 and DeterministicReal=@p3 and DeterministicNumericDefault=@p4 and"
                    + " DeterministicSmallMoney=@p5 and DeterministicMoney=@p6 and DeterministicBit=@p7 and"
                    + " DeterministicSmallint=@p8 and DeterministicBigint=@p9 and DeterministicFloat=@p10 and"
                    + " DeterministicDecimal=@p11 and DeterministicNumeric=@p12 and DeterministicDecimal2=@p13 and"
                    + " DeterministicNumeric2=@p14 ";

            stmt.execute(sql);
        }
    }

    private void testInputProcedure(String sql, String[] values) throws SQLException {
        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.setInt(1, Integer.parseInt(values[3]));
            if (RandomData.returnZero)
                callableStatement.setBigDecimal(2, new BigDecimal(values[8]), 18, 0);
            else
                callableStatement.setBigDecimal(2, new BigDecimal(values[8]));
            callableStatement.setDouble(3, Double.parseDouble(values[5]));
            callableStatement.setFloat(4, Float.parseFloat(values[7]));
            if (RandomData.returnZero)
                callableStatement.setBigDecimal(5, new BigDecimal(values[10]), 18, 0); // numeric(18,0)
            else
                callableStatement.setBigDecimal(5, new BigDecimal(values[10])); // numeric(18,0)
            callableStatement.setSmallMoney(6, new BigDecimal(values[12]));
            callableStatement.setMoney(7, new BigDecimal(values[13]));
            if (values[0].equalsIgnoreCase(Boolean.TRUE.toString()))
                callableStatement.setBoolean(8, true);
            else
                callableStatement.setBoolean(8, false);
            callableStatement.setShort(9, Short.parseShort(values[2])); // smallint
            callableStatement.setLong(10, Long.parseLong(values[4])); // bigint
            callableStatement.setDouble(11, Double.parseDouble(values[6])); // float30
            callableStatement.setBigDecimal(12, new BigDecimal(values[9]), 10, 5); // decimal(10,5)
            callableStatement.setBigDecimal(13, new BigDecimal(values[11]), 8, 2); // numeric(8,2)
            callableStatement.setBigDecimal(14, new BigDecimal(values[14]), 28, 4);
            callableStatement.setBigDecimal(15, new BigDecimal(values[15]), 28, 4);

            try (SQLServerResultSet rs = (SQLServerResultSet) callableStatement.executeQuery()) {
                rs.next();
                assertEquals(rs.getString(1), values[3], "" + TestResource.getResource("R_inputParamFailed"));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createInputProcedure2() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(inputProcedure2) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputProcedure2);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputProcedure2)
                    + " @p0 varchar(50), @p1 uniqueidentifier, @p2 varchar(max), @p3 nchar(30), @p4 nvarchar(60), @p5 nvarchar(max), "
                    + " @p6 varchar(8000), @p7 nvarchar(4000)" + " AS"
                    + " SELECT top 1 RandomizedVarchar, DeterministicUniqueidentifier, DeterministicVarcharMax, RandomizedNchar, "
                    + " DeterministicNvarchar, DeterministicNvarcharMax, DeterministicVarchar8000, RandomizedNvarchar4000  FROM "
                    + AbstractSQLGenerator.escapeIdentifier(CHAR_TABLE_AE)
                    + " where DeterministicVarchar = @p0 and DeterministicUniqueidentifier =@p1";

            stmt.execute(sql);
        }
    }

    private void testInputProcedure2(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.setString(1, charValues[1]);
            callableStatement.setUniqueIdentifier(2, charValues[6]);
            callableStatement.setString(3, charValues[2]);
            callableStatement.setNString(4, charValues[3]);
            callableStatement.setNString(5, charValues[4]);
            callableStatement.setNString(6, charValues[5]);
            callableStatement.setString(7, charValues[7]);
            callableStatement.setNString(8, charValues[8]);

            try (SQLServerResultSet rs = (SQLServerResultSet) callableStatement.executeQuery()) {
                rs.next();
                assertEquals(rs.getString(1).trim(), charValues[1], TestResource.getResource("R_inputParamFailed"));
                assertEquals(rs.getUniqueIdentifier(2), charValues[6].toUpperCase(),
                        TestResource.getResource("R_inputParamFailed"));
                assertEquals(rs.getString(3).trim(), charValues[2], TestResource.getResource("R_inputParamFailed"));
                assertEquals(rs.getString(4).trim(), charValues[3], TestResource.getResource("R_inputParamFailed"));
                assertEquals(rs.getString(5).trim(), charValues[4], TestResource.getResource("R_inputParamFailed"));
                assertEquals(rs.getString(6).trim(), charValues[5], TestResource.getResource("R_inputParamFailed"));
                assertEquals(rs.getString(7).trim(), charValues[7], TestResource.getResource("R_inputParamFailed"));
                assertEquals(rs.getString(8).trim(), charValues[8], TestResource.getResource("R_inputParamFailed"));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createOutputProcedure3() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(outputProcedure3) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedure3);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedure3)
                    + " @p0 int OUTPUT, @p1 int OUTPUT " + " AS" + " SELECT top 1 @p0=DeterministicInt FROM "
                    + AbstractSQLGenerator.escapeIdentifier(table3) + " SELECT top 1 @p1=RandomizedInt FROM "
                    + AbstractSQLGenerator.escapeIdentifier(table4);

            stmt.execute(sql);
        }
    }

    private void testOutputProcedure3RandomOrder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);

            callableStatement.execute();

            int intValue2 = callableStatement.getInt(2);
            assertEquals("" + intValue2, numericValues[3], TestResource.getResource("R_outputParamFailed"));

            int intValue = callableStatement.getInt(1);
            assertEquals("" + intValue, numericValues[3], TestResource.getResource("R_outputParamFailed"));

            int intValue3 = callableStatement.getInt(2);
            assertEquals("" + intValue3, numericValues[3], TestResource.getResource("R_outputParamFailed"));

            int intValue4 = callableStatement.getInt(2);
            assertEquals("" + intValue4, numericValues[3], TestResource.getResource("R_outputParamFailed"));

            int intValue5 = callableStatement.getInt(1);
            assertEquals("" + intValue5, numericValues[3], TestResource.getResource("R_outputParamFailed"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testOutputProcedure3Inorder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);

            callableStatement.execute();

            int intValue = callableStatement.getInt(1);
            assertEquals("" + intValue, numericValues[3], TestResource.getResource("R_outputParamFailed"));

            int intValue2 = callableStatement.getInt(2);
            assertEquals("" + intValue2, numericValues[3], TestResource.getResource("R_outputParamFailed"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testOutputProcedure3ReverseOrder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);

            callableStatement.execute();

            int intValue2 = callableStatement.getInt(2);
            assertEquals("" + intValue2, numericValues[3], TestResource.getResource("R_outputParamFailed"));

            int intValue = callableStatement.getInt(1);
            assertEquals("" + intValue, numericValues[3], TestResource.getResource("R_outputParamFailed"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createOutputProcedure2() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(outputProcedure2) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedure2);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedure2)
                    + " @p0 int OUTPUT, @p1 int OUTPUT, @p2 smallint OUTPUT, @p3 smallint OUTPUT, @p4 tinyint OUTPUT, @p5 tinyint OUTPUT, @p6 smallmoney OUTPUT,"
                    + " @p7 smallmoney OUTPUT, @p8 money OUTPUT, @p9 money OUTPUT " + " AS"
                    + " SELECT top 1 @p0=PlainInt, @p1=DeterministicInt, @p2=PlainSmallint,"
                    + " @p3=RandomizedSmallint, @p4=PlainTinyint, @p5=DeterministicTinyint, @p6=DeterministicSmallMoney, @p7=PlainSmallMoney,"
                    + " @p8=PlainMoney, @p9=DeterministicMoney FROM " + AbstractSQLGenerator.escapeIdentifier(table3);

            stmt.execute(sql);
        }
    }

    private void testOutputProcedure2RandomOrder(String sql, String[] values) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(3, java.sql.Types.SMALLINT);
            callableStatement.registerOutParameter(4, java.sql.Types.SMALLINT);
            callableStatement.registerOutParameter(5, java.sql.Types.TINYINT);
            callableStatement.registerOutParameter(6, java.sql.Types.TINYINT);
            callableStatement.registerOutParameter(7, microsoft.sql.Types.SMALLMONEY);
            callableStatement.registerOutParameter(8, microsoft.sql.Types.SMALLMONEY);
            callableStatement.registerOutParameter(9, microsoft.sql.Types.MONEY);
            callableStatement.registerOutParameter(10, microsoft.sql.Types.MONEY);

            callableStatement.execute();

            BigDecimal ecnryptedSmallMoney = callableStatement.getSmallMoney(7);
            assertEquals("" + ecnryptedSmallMoney, values[12], TestResource.getResource("R_outputParamFailed"));

            short encryptedSmallint = callableStatement.getShort(4);
            assertEquals("" + encryptedSmallint, values[2], TestResource.getResource("R_outputParamFailed"));

            BigDecimal SmallMoneyValue = callableStatement.getSmallMoney(8);
            assertEquals("" + SmallMoneyValue, values[12], TestResource.getResource("R_outputParamFailed"));

            short encryptedTinyint = callableStatement.getShort(6);
            assertEquals("" + encryptedTinyint, values[1], TestResource.getResource("R_outputParamFailed"));

            short tinyintValue = callableStatement.getShort(5);
            assertEquals("" + tinyintValue, values[1], TestResource.getResource("R_outputParamFailed"));

            BigDecimal encryptedMoneyValue = callableStatement.getMoney(9);
            assertEquals("" + encryptedMoneyValue, values[13], TestResource.getResource("R_outputParamFailed"));

            short smallintValue = callableStatement.getShort(3);
            assertEquals("" + smallintValue, values[2], TestResource.getResource("R_outputParamFailed"));

            int intValue = callableStatement.getInt(1);
            assertEquals("" + intValue, values[3], TestResource.getResource("R_outputParamFailed"));

            BigDecimal encryptedSmallMoney = callableStatement.getMoney(10);
            assertEquals("" + encryptedSmallMoney, values[13], TestResource.getResource("R_outputParamFailed"));

            int encryptedInt = callableStatement.getInt(2);
            assertEquals("" + encryptedInt, values[3], TestResource.getResource("R_outputParamFailed"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testOutputProcedure2Inorder(String sql, String[] values) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(3, java.sql.Types.SMALLINT);
            callableStatement.registerOutParameter(4, java.sql.Types.SMALLINT);
            callableStatement.registerOutParameter(5, java.sql.Types.TINYINT);
            callableStatement.registerOutParameter(6, java.sql.Types.TINYINT);
            callableStatement.registerOutParameter(7, microsoft.sql.Types.SMALLMONEY);
            callableStatement.registerOutParameter(8, microsoft.sql.Types.SMALLMONEY);
            callableStatement.registerOutParameter(9, microsoft.sql.Types.MONEY);
            callableStatement.registerOutParameter(10, microsoft.sql.Types.MONEY);
            callableStatement.execute();

            int intValue = callableStatement.getInt(1);
            assertEquals("" + intValue, values[3], TestResource.getResource("R_outputParamFailed"));

            int encryptedInt = callableStatement.getInt(2);
            assertEquals("" + encryptedInt, values[3], TestResource.getResource("R_outputParamFailed"));

            short smallintValue = callableStatement.getShort(3);
            assertEquals("" + smallintValue, values[2], TestResource.getResource("R_outputParamFailed"));

            short encryptedSmallint = callableStatement.getShort(4);
            assertEquals("" + encryptedSmallint, values[2], TestResource.getResource("R_outputParamFailed"));

            short tinyintValue = callableStatement.getShort(5);
            assertEquals("" + tinyintValue, values[1], TestResource.getResource("R_outputParamFailed"));

            short encryptedTinyint = callableStatement.getShort(6);
            assertEquals("" + encryptedTinyint, values[1], TestResource.getResource("R_outputParamFailed"));

            BigDecimal encryptedSmallMoney = callableStatement.getSmallMoney(7);
            assertEquals("" + encryptedSmallMoney, values[12], TestResource.getResource("R_outputParamFailed"));

            BigDecimal SmallMoneyValue = callableStatement.getSmallMoney(8);
            assertEquals("" + SmallMoneyValue, values[12], TestResource.getResource("R_outputParamFailed"));

            BigDecimal MoneyValue = callableStatement.getMoney(9);
            assertEquals("" + MoneyValue, values[13], TestResource.getResource("R_outputParamFailed"));

            BigDecimal encryptedMoney = callableStatement.getMoney(10);
            assertEquals("" + encryptedMoney, values[13], TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testOutputProcedure2ReverseOrder(String sql, String[] values) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(3, java.sql.Types.SMALLINT);
            callableStatement.registerOutParameter(4, java.sql.Types.SMALLINT);
            callableStatement.registerOutParameter(5, java.sql.Types.TINYINT);
            callableStatement.registerOutParameter(6, java.sql.Types.TINYINT);
            callableStatement.registerOutParameter(7, microsoft.sql.Types.SMALLMONEY);
            callableStatement.registerOutParameter(8, microsoft.sql.Types.SMALLMONEY);
            callableStatement.registerOutParameter(9, microsoft.sql.Types.MONEY);
            callableStatement.registerOutParameter(10, microsoft.sql.Types.MONEY);

            callableStatement.execute();

            BigDecimal encryptedMoney = callableStatement.getMoney(10);
            assertEquals("" + encryptedMoney, values[13], TestResource.getResource("R_outputParamFailed"));

            BigDecimal MoneyValue = callableStatement.getMoney(9);
            assertEquals("" + MoneyValue, values[13], TestResource.getResource("R_outputParamFailed"));

            BigDecimal SmallMoneyValue = callableStatement.getSmallMoney(8);
            assertEquals("" + SmallMoneyValue, values[12], TestResource.getResource("R_outputParamFailed"));

            BigDecimal encryptedSmallMoney = callableStatement.getSmallMoney(7);
            assertEquals("" + encryptedSmallMoney, values[12], TestResource.getResource("R_outputParamFailed"));

            short encryptedTinyint = callableStatement.getShort(6);
            assertEquals("" + encryptedTinyint, values[1], TestResource.getResource("R_outputParamFailed"));

            short tinyintValue = callableStatement.getShort(5);
            assertEquals("" + tinyintValue, values[1], TestResource.getResource("R_outputParamFailed"));

            short encryptedSmallint = callableStatement.getShort(4);
            assertEquals("" + encryptedSmallint, values[2], TestResource.getResource("R_outputParamFailed"));

            short smallintValue = callableStatement.getShort(3);
            assertEquals("" + smallintValue, values[2], TestResource.getResource("R_outputParamFailed"));

            int encryptedInt = callableStatement.getInt(2);
            assertEquals("" + encryptedInt, values[3], TestResource.getResource("R_outputParamFailed"));

            int intValue = callableStatement.getInt(1);
            assertEquals("" + intValue, values[3], TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createOutputProcedure() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(outputProcedure) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedure);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedure)
                    + " @p0 int OUTPUT, @p1 float OUTPUT, @p2 smallint OUTPUT, "
                    + "@p3 bigint OUTPUT, @p4 tinyint OUTPUT, @p5 smallmoney OUTPUT, @p6 money OUTPUT " + " AS"
                    + " SELECT top 1 @p0=RandomizedInt, @p1=DeterministicFloatDefault, @p2=RandomizedSmallint,"
                    + " @p3=RandomizedBigint, @p4=DeterministicTinyint, @p5=DeterministicSmallMoney, @p6=DeterministicMoney FROM "
                    + AbstractSQLGenerator.escapeIdentifier(table3);

            stmt.execute(sql);
        }
    }

    private void testOutputProcedureRandomOrder(String sql, String[] values) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(2, java.sql.Types.DOUBLE);
            callableStatement.registerOutParameter(3, java.sql.Types.SMALLINT);
            callableStatement.registerOutParameter(4, java.sql.Types.BIGINT);
            callableStatement.registerOutParameter(5, java.sql.Types.TINYINT);
            callableStatement.registerOutParameter(6, microsoft.sql.Types.SMALLMONEY);
            callableStatement.registerOutParameter(7, microsoft.sql.Types.MONEY);

            callableStatement.execute();

            double floatValue0 = callableStatement.getDouble(2);
            assertEquals("" + floatValue0, "" + values[5], TestResource.getResource("R_outputParamFailed"));

            long bigintValue = callableStatement.getLong(4);
            assertEquals("" + bigintValue, values[4], TestResource.getResource("R_outputParamFailed"));

            short tinyintValue = callableStatement.getShort(5); // tinyint
            assertEquals("" + tinyintValue, values[1], TestResource.getResource("R_outputParamFailed"));

            double floatValue1 = callableStatement.getDouble(2);
            assertEquals("" + floatValue1, "" + values[5], TestResource.getResource("R_outputParamFailed"));

            int intValue2 = callableStatement.getInt(1);
            assertEquals("" + intValue2, "" + values[3], TestResource.getResource("R_outputParamFailed"));

            double floatValue2 = callableStatement.getDouble(2);
            assertEquals("" + floatValue2, "" + values[5], TestResource.getResource("R_outputParamFailed"));

            short shortValue3 = callableStatement.getShort(3); // smallint
            assertEquals("" + shortValue3, "" + values[2], TestResource.getResource("R_outputParamFailed"));

            short shortValue32 = callableStatement.getShort(3);
            assertEquals("" + shortValue32, "" + values[2], TestResource.getResource("R_outputParamFailed"));

            BigDecimal smallmoney1 = callableStatement.getSmallMoney(6);
            assertEquals("" + smallmoney1, "" + values[12], TestResource.getResource("R_outputParamFailed"));
            BigDecimal money1 = callableStatement.getMoney(7);
            assertEquals("" + money1, "" + values[13], TestResource.getResource("R_outputParamFailed"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testOutputProcedureInorder(String sql, String[] values) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(2, java.sql.Types.DOUBLE);
            callableStatement.registerOutParameter(3, java.sql.Types.SMALLINT);
            callableStatement.registerOutParameter(4, java.sql.Types.BIGINT);
            callableStatement.registerOutParameter(5, java.sql.Types.TINYINT);
            callableStatement.registerOutParameter(6, microsoft.sql.Types.SMALLMONEY);
            callableStatement.registerOutParameter(7, microsoft.sql.Types.MONEY);

            callableStatement.execute();

            int intValue2 = callableStatement.getInt(1);
            assertEquals("" + intValue2, values[3], TestResource.getResource("R_outputParamFailed"));

            double floatValue0 = callableStatement.getDouble(2);
            assertEquals("" + floatValue0, values[5], TestResource.getResource("R_outputParamFailed"));

            short shortValue3 = callableStatement.getShort(3);
            assertEquals("" + shortValue3, values[2], TestResource.getResource("R_outputParamFailed"));

            long bigintValue = callableStatement.getLong(4);
            assertEquals("" + bigintValue, values[4], TestResource.getResource("R_outputParamFailed"));

            short tinyintValue = callableStatement.getShort(5);
            assertEquals("" + tinyintValue, values[1], TestResource.getResource("R_outputParamFailed"));

            BigDecimal smallMoney1 = callableStatement.getSmallMoney(6);
            assertEquals("" + smallMoney1, values[12], TestResource.getResource("R_outputParamFailed"));

            BigDecimal money1 = callableStatement.getMoney(7);
            assertEquals("" + money1, values[13], TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testOutputProcedureReverseOrder(String sql, String[] values) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(2, java.sql.Types.DOUBLE);
            callableStatement.registerOutParameter(3, java.sql.Types.SMALLINT);
            callableStatement.registerOutParameter(4, java.sql.Types.BIGINT);
            callableStatement.registerOutParameter(5, java.sql.Types.TINYINT);
            callableStatement.registerOutParameter(6, microsoft.sql.Types.SMALLMONEY);
            callableStatement.registerOutParameter(7, microsoft.sql.Types.MONEY);
            callableStatement.execute();

            BigDecimal smallMoney1 = callableStatement.getSmallMoney(6);
            assertEquals("" + smallMoney1, values[12], TestResource.getResource("R_outputParamFailed"));

            BigDecimal money1 = callableStatement.getMoney(7);
            assertEquals("" + money1, values[13], TestResource.getResource("R_outputParamFailed"));

            short tinyintValue = callableStatement.getShort(5);
            assertEquals("" + tinyintValue, values[1], TestResource.getResource("R_outputParamFailed"));

            long bigintValue = callableStatement.getLong(4);
            assertEquals("" + bigintValue, values[4], TestResource.getResource("R_outputParamFailed"));

            short shortValue3 = callableStatement.getShort(3);
            assertEquals("" + shortValue3, values[2], TestResource.getResource("R_outputParamFailed"));

            double floatValue0 = callableStatement.getDouble(2);
            assertEquals("" + floatValue0, values[5], TestResource.getResource("R_outputParamFailed"));

            int intValue2 = callableStatement.getInt(1);
            assertEquals("" + intValue2, values[3], TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createInOutProcedure() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(inoutProcedure) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inoutProcedure);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inoutProcedure) + " @p0 int OUTPUT"
                    + " AS" + " SELECT top 1 @p0=DeterministicInt FROM " + AbstractSQLGenerator.escapeIdentifier(table3)
                    + " where DeterministicInt=@p0";

            stmt.execute(sql);
        }
    }

    private void testInOutProcedure(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.setInt(1, Integer.parseInt(numericValues[3]));
            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.execute();

            int intValue = callableStatement.getInt(1);

            assertEquals("" + intValue, numericValues[3], "Test for Inout parameter fails.\n");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createMixedProcedure() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(mixedProcedure) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(mixedProcedure);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(mixedProcedure)
                    + " @p0 int OUTPUT, @p1 float OUTPUT, @p3 decimal " + " AS"
                    + " SELECT top 1 @p0=DeterministicInt2, @p1=RandomizedFloatDefault FROM "
                    + AbstractSQLGenerator.escapeIdentifier(table3)
                    + " where DeterministicInt=@p0 and DeterministicDecimalDefault=@p3" + " return 123";

            stmt.execute(sql);
        }
    }

    private void testMixedProcedure(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.setInt(2, Integer.parseInt(numericValues[3]));
            callableStatement.registerOutParameter(2, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(3, java.sql.Types.DOUBLE);
            if (RandomData.returnZero)
                callableStatement.setBigDecimal(4, new BigDecimal(numericValues[8]), 18, 0);
            else
                callableStatement.setBigDecimal(4, new BigDecimal(numericValues[8]));
            callableStatement.execute();

            int intValue = callableStatement.getInt(2);
            assertEquals("" + intValue, numericValues[3], "Test for Inout parameter fails.\n");

            double floatValue = callableStatement.getDouble(3);
            assertEquals("" + floatValue, numericValues[5], TestResource.getResource("R_outputParamFailed"));

            int returnedValue = callableStatement.getInt(1);
            assertEquals("" + returnedValue, "" + 123, "Test for Inout parameter fails.\n");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createMixedProcedure2() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(mixedProcedure2) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(mixedProcedure2);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(mixedProcedure2)
                    + " @p0 int OUTPUT, @p1 float OUTPUT, @p3 int, @p4 float " + " AS"
                    + " SELECT top 1 @p0=DeterministicInt, @p1=PlainFloatDefault FROM "
                    + AbstractSQLGenerator.escapeIdentifier(table3)
                    + " where PlainInt=@p3 and DeterministicFloatDefault=@p4";

            stmt.execute(sql);
        }
    }

    private void testMixedProcedure2RandomOrder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(2, java.sql.Types.FLOAT);
            callableStatement.setInt(3, Integer.parseInt(numericValues[3]));
            callableStatement.setDouble(4, Double.parseDouble(numericValues[5]));
            callableStatement.execute();

            double floatValue = callableStatement.getDouble(2);
            assertEquals("" + floatValue, numericValues[5], TestResource.getResource("R_outputParamFailed"));

            int intValue = callableStatement.getInt(1);
            assertEquals("" + intValue, numericValues[3], TestResource.getResource("R_outputParamFailed"));

            double floatValue2 = callableStatement.getDouble(2);
            assertEquals("" + floatValue2, numericValues[5], TestResource.getResource("R_outputParamFailed"));

            int intValue2 = callableStatement.getInt(1);
            assertEquals("" + intValue2, numericValues[3], TestResource.getResource("R_outputParamFailed"));

            int intValue3 = callableStatement.getInt(1);
            assertEquals("" + intValue3, numericValues[3], TestResource.getResource("R_outputParamFailed"));

            double floatValue3 = callableStatement.getDouble(2);
            assertEquals("" + floatValue3, numericValues[5], TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testMixedProcedure2Inorder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(2, java.sql.Types.FLOAT);
            callableStatement.setInt(3, Integer.parseInt(numericValues[3]));
            callableStatement.setDouble(4, Double.parseDouble(numericValues[5]));
            callableStatement.execute();

            int intValue = callableStatement.getInt(1);
            assertEquals("" + intValue, numericValues[3], TestResource.getResource("R_outputParamFailed"));

            double floatValue = callableStatement.getDouble(2);
            assertEquals("" + floatValue, numericValues[5], TestResource.getResource("R_outputParamFailed"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createMixedProcedure3() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(mixedProcedure3) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(mixedProcedure3);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(mixedProcedure3)
                    + " @p0 bigint OUTPUT, @p1 float OUTPUT, @p2 int OUTPUT, @p3 smallint" + " AS"
                    + " SELECT top 1 @p0=PlainBigint, @p1=PlainFloatDefault FROM "
                    + AbstractSQLGenerator.escapeIdentifier(table3) + " where PlainInt=@p2 and PlainSmallint=@p3";

            stmt.execute(sql);
        }
    }

    private void testMixedProcedure3RandomOrder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.BIGINT);
            callableStatement.registerOutParameter(2, java.sql.Types.FLOAT);
            callableStatement.setInt(3, Integer.parseInt(numericValues[3]));
            callableStatement.setShort(4, Short.parseShort(numericValues[2]));
            callableStatement.execute();

            double floatValue = callableStatement.getDouble(2);
            assertEquals("" + floatValue, numericValues[5], TestResource.getResource("R_outputParamFailed"));

            long bigintValue = callableStatement.getLong(1);
            assertEquals("" + bigintValue, numericValues[4], TestResource.getResource("R_outputParamFailed"));

            long bigintValue1 = callableStatement.getLong(1);
            assertEquals("" + bigintValue1, numericValues[4], TestResource.getResource("R_outputParamFailed"));

            double floatValue2 = callableStatement.getDouble(2);
            assertEquals("" + floatValue2, numericValues[5], TestResource.getResource("R_outputParamFailed"));

            double floatValue3 = callableStatement.getDouble(2);
            assertEquals("" + floatValue3, numericValues[5], TestResource.getResource("R_outputParamFailed"));

            long bigintValue3 = callableStatement.getLong(1);
            assertEquals("" + bigintValue3, numericValues[4], TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testMixedProcedure3Inorder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.BIGINT);
            callableStatement.registerOutParameter(2, java.sql.Types.FLOAT);
            callableStatement.setInt(3, Integer.parseInt(numericValues[3]));
            callableStatement.setShort(4, Short.parseShort(numericValues[2]));
            callableStatement.execute();

            long bigintValue = callableStatement.getLong(1);
            assertEquals("" + bigintValue, numericValues[4], TestResource.getResource("R_outputParamFailed"));

            double floatValue = callableStatement.getDouble(2);
            assertEquals("" + floatValue, numericValues[5], TestResource.getResource("R_outputParamFailed"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testMixedProcedure3ReverseOrder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.BIGINT);
            callableStatement.registerOutParameter(2, java.sql.Types.FLOAT);
            callableStatement.setInt(3, Integer.parseInt(numericValues[3]));
            callableStatement.setShort(4, Short.parseShort(numericValues[2]));
            callableStatement.execute();

            double floatValue = callableStatement.getDouble(2);
            assertEquals("" + floatValue, numericValues[5], TestResource.getResource("R_outputParamFailed"));

            long bigintValue = callableStatement.getLong(1);
            assertEquals("" + bigintValue, numericValues[4], TestResource.getResource("R_outputParamFailed"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createMixedProcedureNumericPrcisionScale() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(mixedProcedureNumericPrcisionScale)
                + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)" + " DROP PROCEDURE "
                + AbstractSQLGenerator.escapeIdentifier(mixedProcedureNumericPrcisionScale);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(mixedProcedureNumericPrcisionScale)
                    + " @p1 decimal(18,0) OUTPUT, @p2 decimal(10,5) OUTPUT, @p3 numeric(18, 0) OUTPUT, @p4 numeric(8,2) OUTPUT "
                    + " AS" + " SELECT top 1 @p1=RandomizedDecimalDefault, @p2=DeterministicDecimal,"
                    + " @p3=RandomizedNumericDefault, @p4=DeterministicNumeric FROM "
                    + AbstractSQLGenerator.escapeIdentifier(table3)
                    + " where DeterministicDecimal=@p2 and DeterministicNumeric=@p4" + " return 123";

            stmt.execute(sql);
        }
    }

    private void testMixedProcedureNumericPrcisionScaleInorder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.DECIMAL, 18, 0);
            callableStatement.registerOutParameter(2, java.sql.Types.DECIMAL, 10, 5);
            callableStatement.registerOutParameter(3, java.sql.Types.NUMERIC, 18, 0);
            callableStatement.registerOutParameter(4, java.sql.Types.NUMERIC, 8, 2);
            callableStatement.setBigDecimal(2, new BigDecimal(numericValues[9]), 10, 5);
            callableStatement.setBigDecimal(4, new BigDecimal(numericValues[11]), 8, 2);
            callableStatement.execute();

            BigDecimal value1 = callableStatement.getBigDecimal(1);
            assertEquals(value1, new BigDecimal(numericValues[8]), "Test for input output parameter fails.\n");

            BigDecimal value2 = callableStatement.getBigDecimal(2);
            assertEquals(value2, new BigDecimal(numericValues[9]), "Test for input output parameter fails.\n");

            BigDecimal value3 = callableStatement.getBigDecimal(3);
            assertEquals(value3, new BigDecimal(numericValues[10]), "Test for input output parameter fails.\n");

            BigDecimal value4 = callableStatement.getBigDecimal(4);
            assertEquals(value4, new BigDecimal(numericValues[11]), "Test for input output parameter fails.\n");

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testMixedProcedureNumericPrcisionScaleParameterName(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter("p1", java.sql.Types.DECIMAL, 18, 0);
            callableStatement.registerOutParameter("p2", java.sql.Types.DECIMAL, 10, 5);
            callableStatement.registerOutParameter("p3", java.sql.Types.NUMERIC, 18, 0);
            callableStatement.registerOutParameter("p4", java.sql.Types.NUMERIC, 8, 2);
            callableStatement.setBigDecimal("p2", new BigDecimal(numericValues[9]), 10, 5);
            callableStatement.setBigDecimal("p4", new BigDecimal(numericValues[11]), 8, 2);
            callableStatement.execute();

            BigDecimal value1 = callableStatement.getBigDecimal(1);
            assertEquals(value1, new BigDecimal(numericValues[8]), "Test for input output parameter fails.\n");

            BigDecimal value2 = callableStatement.getBigDecimal(2);
            assertEquals(value2, new BigDecimal(numericValues[9]), "Test for input output parameter fails.\n");

            BigDecimal value3 = callableStatement.getBigDecimal(3);
            assertEquals(value3, new BigDecimal(numericValues[10]), "Test for input output parameter fails.\n");

            BigDecimal value4 = callableStatement.getBigDecimal(4);
            assertEquals(value4, new BigDecimal(numericValues[11]), "Test for input output parameter fails.\n");

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createOutputProcedureChar() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(outputProcedureChar) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedureChar);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedureChar)
                    + " @p0 char(20) OUTPUT,@p1 varchar(50) OUTPUT,@p2 nchar(30) OUTPUT,"
                    + "@p3 nvarchar(60) OUTPUT, @p4 uniqueidentifier OUTPUT, @p5 varchar(max) OUTPUT, @p6 nvarchar(max) OUTPUT, @p7 varchar(8000) OUTPUT, @p8 nvarchar(4000) OUTPUT"
                    + " AS" + " SELECT top 1 @p0=DeterministicChar,@p1=RandomizedVarChar,@p2=RandomizedNChar,"
                    + " @p3=DeterministicNVarChar, @p4=DeterministicUniqueidentifier, @p5=DeterministicVarcharMax,"
                    + " @p6=DeterministicNvarcharMax, @p7=DeterministicVarchar8000, @p8=RandomizedNvarchar4000  FROM  "
                    + AbstractSQLGenerator.escapeIdentifier(CHAR_TABLE_AE);

            stmt.execute(sql);
        }
    }

    private void testOutputProcedureCharInorder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.CHAR, 20, 0);
            callableStatement.registerOutParameter(2, java.sql.Types.VARCHAR, 50, 0);
            callableStatement.registerOutParameter(3, java.sql.Types.NCHAR, 30, 0);
            callableStatement.registerOutParameter(4, java.sql.Types.NVARCHAR, 60, 0);
            callableStatement.registerOutParameter(5, microsoft.sql.Types.GUID);
            callableStatement.registerOutParameter(6, java.sql.Types.LONGVARCHAR);
            callableStatement.registerOutParameter(7, java.sql.Types.LONGNVARCHAR);
            callableStatement.registerOutParameter(8, java.sql.Types.VARCHAR, 8000, 0);
            callableStatement.registerOutParameter(9, java.sql.Types.NVARCHAR, 4000, 0);

            callableStatement.execute();
            String charValue = callableStatement.getString(1).trim();
            assertEquals(charValue, charValues[0], TestResource.getResource("R_outputParamFailed"));

            String varcharValue = callableStatement.getString(2).trim();
            assertEquals(varcharValue, charValues[1], TestResource.getResource("R_outputParamFailed"));

            String ncharValue = callableStatement.getString(3).trim();
            assertEquals(ncharValue, charValues[3], TestResource.getResource("R_outputParamFailed"));

            String nvarcharValue = callableStatement.getString(4).trim();
            assertEquals(nvarcharValue, charValues[4], TestResource.getResource("R_outputParamFailed"));

            String uniqueIdentifierValue = callableStatement.getString(5).trim();
            assertEquals(uniqueIdentifierValue.toLowerCase(), charValues[6],
                    TestResource.getResource("R_outputParamFailed"));

            String varcharValuemax = callableStatement.getString(6).trim();
            assertEquals(varcharValuemax, charValues[2], TestResource.getResource("R_outputParamFailed"));

            String nvarcharValuemax = callableStatement.getString(7).trim();
            assertEquals(nvarcharValuemax, charValues[5], TestResource.getResource("R_outputParamFailed"));

            String varcharValue8000 = callableStatement.getString(8).trim();
            assertEquals(varcharValue8000, charValues[7], TestResource.getResource("R_outputParamFailed"));

            String nvarcharValue4000 = callableStatement.getNString(9).trim();
            assertEquals(nvarcharValue4000, charValues[8], TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testOutputProcedureCharInorderObject(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.CHAR, 20, 0);
            callableStatement.registerOutParameter(2, java.sql.Types.VARCHAR, 50, 0);
            callableStatement.registerOutParameter(3, java.sql.Types.NCHAR, 30, 0);
            callableStatement.registerOutParameter(4, java.sql.Types.NVARCHAR, 60, 0);
            callableStatement.registerOutParameter(5, microsoft.sql.Types.GUID);
            callableStatement.registerOutParameter(6, java.sql.Types.LONGVARCHAR);
            callableStatement.registerOutParameter(7, java.sql.Types.LONGNVARCHAR);
            callableStatement.registerOutParameter(8, java.sql.Types.VARCHAR, 8000, 0);
            callableStatement.registerOutParameter(9, java.sql.Types.NVARCHAR, 4000, 0);

            callableStatement.execute();

            String charValue = (String) callableStatement.getObject(1);
            assertEquals(charValue.trim(), charValues[0], TestResource.getResource("R_outputParamFailed"));

            String varcharValue = (String) callableStatement.getObject(2);
            assertEquals(varcharValue.trim(), charValues[1], TestResource.getResource("R_outputParamFailed"));

            String ncharValue = (String) callableStatement.getObject(3);
            assertEquals(ncharValue.trim(), charValues[3], TestResource.getResource("R_outputParamFailed"));

            String nvarcharValue = (String) callableStatement.getObject(4);
            assertEquals(nvarcharValue.trim(), charValues[4], TestResource.getResource("R_outputParamFailed"));

            String uniqueIdentifierValue = (String) callableStatement.getObject(5);
            assertEquals(uniqueIdentifierValue.toLowerCase(), charValues[6],
                    TestResource.getResource("R_outputParamFailed"));

            String varcharValuemax = (String) callableStatement.getObject(6);

            assertEquals(varcharValuemax, charValues[2], TestResource.getResource("R_outputParamFailed"));

            String nvarcharValuemax = (String) callableStatement.getObject(7);

            assertEquals(nvarcharValuemax.trim(), charValues[5], TestResource.getResource("R_outputParamFailed"));

            String varcharValue8000 = (String) callableStatement.getObject(8);
            assertEquals(varcharValue8000, charValues[7], TestResource.getResource("R_outputParamFailed"));

            String nvarcharValue4000 = (String) callableStatement.getObject(9);
            assertEquals(nvarcharValue4000, charValues[8], TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createOutputProcedureNumeric() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(outputProcedureNumeric)
                + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)" + " DROP PROCEDURE "
                + AbstractSQLGenerator.escapeIdentifier(outputProcedureNumeric);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedureNumeric)
                    + " @p0 bit OUTPUT, @p1 tinyint OUTPUT, @p2 smallint OUTPUT, @p3 int OUTPUT,"
                    + " @p4 bigint OUTPUT, @p5 float OUTPUT, @p6 float(30) output, @p7 real output, @p8 decimal(18, 0) output, @p9 decimal(10,5) output,"
                    + " @p10 numeric(18, 0) output, @p11 numeric(8,2) output, @p12 smallmoney output, @p13 money output, @p14 decimal(28,4) output, @p15 numeric(28,4) output"
                    + " AS" + " SELECT top 1 @p0=DeterministicBit, @p1=RandomizedTinyint, @p2=DeterministicSmallint,"
                    + " @p3=RandomizedInt, @p4=DeterministicBigint, @p5=RandomizedFloatDefault, @p6=DeterministicFloat,"
                    + " @p7=RandomizedReal, @p8=DeterministicDecimalDefault, @p9=RandomizedDecimal,"
                    + " @p10=DeterministicNumericDefault, @p11=RandomizedNumeric, @p12=RandomizedSmallMoney, @p13=DeterministicMoney,"
                    + " @p14=DeterministicDecimal2, @p15=DeterministicNumeric2 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE);

            stmt.execute(sql);
        }
    }

    private void testOutputProcedureNumericInorder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.BIT);
            callableStatement.registerOutParameter(2, java.sql.Types.TINYINT);
            callableStatement.registerOutParameter(3, java.sql.Types.SMALLINT);
            callableStatement.registerOutParameter(4, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(5, java.sql.Types.BIGINT);
            callableStatement.registerOutParameter(6, java.sql.Types.DOUBLE);
            callableStatement.registerOutParameter(7, java.sql.Types.DOUBLE, 30, 0);
            callableStatement.registerOutParameter(8, java.sql.Types.REAL);
            callableStatement.registerOutParameter(9, java.sql.Types.DECIMAL, 18, 0);
            callableStatement.registerOutParameter(10, java.sql.Types.DECIMAL, 10, 5);
            callableStatement.registerOutParameter(11, java.sql.Types.NUMERIC, 18, 0);
            callableStatement.registerOutParameter(12, java.sql.Types.NUMERIC, 8, 2);
            callableStatement.registerOutParameter(13, microsoft.sql.Types.SMALLMONEY);
            callableStatement.registerOutParameter(14, microsoft.sql.Types.MONEY);
            callableStatement.registerOutParameter(15, java.sql.Types.DECIMAL, 28, 4);
            callableStatement.registerOutParameter(16, java.sql.Types.NUMERIC, 28, 4);

            callableStatement.execute();

            int bitValue = callableStatement.getInt(1);
            if (bitValue == 0)
                assertEquals("" + false, numericValues[0], TestResource.getResource("R_outputParamFailed"));
            else
                assertEquals("" + true, numericValues[0], TestResource.getResource("R_outputParamFailed"));

            short tinyIntValue = callableStatement.getShort(2);
            assertEquals("" + tinyIntValue, numericValues[1], TestResource.getResource("R_outputParamFailed"));

            short smallIntValue = callableStatement.getShort(3);
            assertEquals("" + smallIntValue, numericValues[2], TestResource.getResource("R_outputParamFailed"));

            int intValue = callableStatement.getInt(4);
            assertEquals("" + intValue, numericValues[3], TestResource.getResource("R_outputParamFailed"));

            long bigintValue = callableStatement.getLong(5);
            assertEquals("" + bigintValue, numericValues[4], TestResource.getResource("R_outputParamFailed"));

            double floatDefault = callableStatement.getDouble(6);
            assertEquals("" + floatDefault, numericValues[5], TestResource.getResource("R_outputParamFailed"));

            double floatValue = callableStatement.getDouble(7);
            assertEquals("" + floatValue, numericValues[6], TestResource.getResource("R_outputParamFailed"));

            float realValue = callableStatement.getFloat(8);
            assertEquals("" + realValue, numericValues[7], TestResource.getResource("R_outputParamFailed"));

            BigDecimal decimalDefault = callableStatement.getBigDecimal(9);
            assertEquals(decimalDefault, new BigDecimal(numericValues[8]),
                    TestResource.getResource("R_outputParamFailed"));

            BigDecimal decimalValue = callableStatement.getBigDecimal(10);
            assertEquals(decimalValue, new BigDecimal(numericValues[9]),
                    TestResource.getResource("R_outputParamFailed"));

            BigDecimal numericDefault = callableStatement.getBigDecimal(11);
            assertEquals(numericDefault, new BigDecimal(numericValues[10]),
                    TestResource.getResource("R_outputParamFailed"));

            BigDecimal numericValue = callableStatement.getBigDecimal(12);
            assertEquals(numericValue, new BigDecimal(numericValues[11]),
                    TestResource.getResource("R_outputParamFailed"));

            BigDecimal smallMoneyValue = callableStatement.getSmallMoney(13);
            assertEquals(smallMoneyValue, new BigDecimal(numericValues[12]),
                    TestResource.getResource("R_outputParamFailed"));

            BigDecimal moneyValue = callableStatement.getMoney(14);
            assertEquals(moneyValue, new BigDecimal(numericValues[13]),
                    TestResource.getResource("R_outputParamFailed"));

            BigDecimal decimalValue2 = callableStatement.getBigDecimal(15);
            assertEquals(decimalValue2, new BigDecimal(numericValues[14]),
                    TestResource.getResource("R_outputParamFailed"));

            BigDecimal numericValue2 = callableStatement.getBigDecimal(16);
            assertEquals(numericValue2, new BigDecimal(numericValues[15]),
                    TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testcoerctionsOutputProcedureNumericInorder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.BIT);
            callableStatement.registerOutParameter(2, java.sql.Types.TINYINT);
            callableStatement.registerOutParameter(3, java.sql.Types.SMALLINT);
            callableStatement.registerOutParameter(4, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(5, java.sql.Types.BIGINT);
            callableStatement.registerOutParameter(6, java.sql.Types.DOUBLE);
            callableStatement.registerOutParameter(7, java.sql.Types.DOUBLE, 30, 0);
            callableStatement.registerOutParameter(8, java.sql.Types.REAL);
            callableStatement.registerOutParameter(9, java.sql.Types.DECIMAL, 18, 0);
            callableStatement.registerOutParameter(10, java.sql.Types.DECIMAL, 10, 5);
            callableStatement.registerOutParameter(11, java.sql.Types.NUMERIC, 18, 0);
            callableStatement.registerOutParameter(12, java.sql.Types.NUMERIC, 8, 2);
            callableStatement.registerOutParameter(13, microsoft.sql.Types.SMALLMONEY);
            callableStatement.registerOutParameter(14, microsoft.sql.Types.MONEY);
            callableStatement.registerOutParameter(15, java.sql.Types.DECIMAL, 28, 4);
            callableStatement.registerOutParameter(16, java.sql.Types.NUMERIC, 28, 4);

            callableStatement.execute();

            Class<?>[] boolean_coercions = {Object.class, Short.class, Integer.class, Long.class, Float.class,
                    Double.class, BigDecimal.class, String.class};
            for (int i = 0; i < boolean_coercions.length; i++) {
                Object value = getxxx(1, boolean_coercions[i], callableStatement);
                Object boolVal = null;
                if (value.toString().equals("1") || value.equals(true) || value.toString().equals("1.0"))
                    boolVal = true;
                else if (value.toString().equals("0") || value.equals(false) || value.toString().equals("0.0"))
                    boolVal = false;
                assertEquals("" + boolVal, numericValues[0], TestResource.getResource("R_outputParamFailed"));
            }
            Class<?>[] tinyint_coercions = {Object.class, Short.class, Integer.class, Long.class, Float.class,
                    Double.class, BigDecimal.class, String.class};
            for (int i = 0; i < tinyint_coercions.length; i++) {

                Object tinyIntValue = getxxx(2, tinyint_coercions[i], callableStatement);
                Object x = createValue(tinyint_coercions[i], 1);

                if (x instanceof String)
                    assertEquals("" + tinyIntValue, x, TestResource.getResource("R_outputParamFailed"));
                else
                    assertEquals(tinyIntValue, x, TestResource.getResource("R_outputParamFailed"));
            }

            Class<?>[] smallint_coercions = {Object.class, Short.class, Integer.class, Long.class, Float.class,
                    Double.class, BigDecimal.class, String.class};
            for (int i = 0; i < smallint_coercions.length; i++) {
                Object smallIntValue = getxxx(3, smallint_coercions[i], callableStatement);
                Object x = createValue(smallint_coercions[i], 2);

                if (x instanceof String)
                    assertEquals("" + smallIntValue, x, TestResource.getResource("R_outputParamFailed"));
                else
                    assertEquals(smallIntValue, x, TestResource.getResource("R_outputParamFailed"));
            }

            Class<?>[] int_coercions = {Object.class, Short.class, Integer.class, Long.class, Float.class, Double.class,
                    BigDecimal.class, String.class};
            for (int i = 0; i < int_coercions.length; i++) {
                Object IntValue = getxxx(4, int_coercions[i], callableStatement);
                Object x = createValue(int_coercions[i], 3);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + IntValue, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(IntValue, x, TestResource.getResource("R_outputParamFailed"));
                }
            }

            Class<?>[] bigint_coercions = {Object.class, Short.class, Integer.class, Long.class, Float.class,
                    Double.class, BigDecimal.class, String.class};
            for (int i = 0; i < int_coercions.length; i++) {
                Object bigIntValue = getxxx(5, bigint_coercions[i], callableStatement);
                Object x = createValue(bigint_coercions[i], 4);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + bigIntValue, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(bigIntValue, x, TestResource.getResource("R_outputParamFailed"));
                }
            }

            Class<?>[] float_coercions = {Object.class, Short.class, Integer.class, Long.class, Float.class,
                    Double.class, BigDecimal.class, String.class};
            for (int i = 0; i < float_coercions.length; i++) {
                Object floatDefaultValue = getxxx(6, float_coercions[i], callableStatement);
                Object x = createValue(float_coercions[i], 5);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + floatDefaultValue, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(floatDefaultValue, x, TestResource.getResource("R_outputParamFailed"));
                }
            }

            for (int i = 0; i < float_coercions.length; i++) {
                Object floatValue = getxxx(7, float_coercions[i], callableStatement);
                Object x = createValue(float_coercions[i], 6);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + floatValue, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(floatValue, x, TestResource.getResource("R_outputParamFailed"));
                }
            }

            Class<?>[] real_coercions = {Object.class, Short.class, Integer.class, Long.class, Float.class,
                    BigDecimal.class, String.class};
            for (int i = 0; i < real_coercions.length; i++) {

                Object realValue = getxxx(8, real_coercions[i], callableStatement);

                Object x = createValue(real_coercions[i], 7);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + realValue, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(realValue, x, TestResource.getResource("R_outputParamFailed"));
                }
            }

            Class<?>[] decimalDefault_coercions = {Object.class, Short.class, Integer.class, Long.class, Float.class,
                    Double.class, BigDecimal.class, String.class};
            for (int i = 0; i < decimalDefault_coercions.length; i++) {
                Object decimalDefaultValue = getxxx(9, decimalDefault_coercions[i], callableStatement);
                Object x = createValue(decimalDefault_coercions[i], 8);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + decimalDefaultValue, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(decimalDefaultValue, x, TestResource.getResource("R_outputParamFailed"));
                }
            }

            for (int i = 0; i < decimalDefault_coercions.length; i++) {
                Object decimalValue = getxxx(10, decimalDefault_coercions[i], callableStatement);
                Object x = createValue(decimalDefault_coercions[i], 9);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + decimalValue, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(decimalValue, x, TestResource.getResource("R_outputParamFailed"));
                }
            }

            for (int i = 0; i < decimalDefault_coercions.length; i++) {
                Object numericDefaultValue = getxxx(11, decimalDefault_coercions[i], callableStatement);
                Object x = createValue(decimalDefault_coercions[i], 10);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + numericDefaultValue, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(numericDefaultValue, x, TestResource.getResource("R_outputParamFailed"));
                }
            }

            for (int i = 0; i < decimalDefault_coercions.length; i++) {
                Object numericValue = getxxx(12, decimalDefault_coercions[i], callableStatement);
                Object x = createValue(decimalDefault_coercions[i], 11);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + numericValue, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(numericValue, x, TestResource.getResource("R_outputParamFailed"));
                }
            }

            for (int i = 0; i < decimalDefault_coercions.length; i++) {
                Object smallMoneyValue = getxxx(13, decimalDefault_coercions[i], callableStatement);
                Object x = createValue(decimalDefault_coercions[i], 12);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + smallMoneyValue, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(smallMoneyValue, x, TestResource.getResource("R_outputParamFailed"));
                }
            }

            for (int i = 0; i < decimalDefault_coercions.length; i++) {
                Object moneyValue = getxxx(14, decimalDefault_coercions[i], callableStatement);
                Object x = createValue(decimalDefault_coercions[i], 13);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + moneyValue, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(moneyValue, x, TestResource.getResource("R_outputParamFailed"));
                }
            }
            for (int i = 0; i < decimalDefault_coercions.length; i++) {
                Object decimalValue2 = getxxx(15, decimalDefault_coercions[i], callableStatement);
                Object x = createValue(decimalDefault_coercions[i], 14);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + decimalValue2, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(decimalValue2, x, TestResource.getResource("R_outputParamFailed"));
                }
            }

            for (int i = 0; i < decimalDefault_coercions.length; i++) {
                Object numericValue1 = getxxx(16, decimalDefault_coercions[i], callableStatement);
                Object x = createValue(decimalDefault_coercions[i], 15);
                if (x != null) {
                    if (x instanceof String)
                        assertEquals("" + numericValue1, x, TestResource.getResource("R_outputParamFailed"));
                    else
                        assertEquals(numericValue1, x, TestResource.getResource("R_outputParamFailed"));
                }
            }

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private Object createValue(Class<?> coercion, int index) {
        try {
            if (coercion == Float.class)
                return Float.parseFloat(numericValues[index]);
            if (coercion == Integer.class)
                return Integer.parseInt(numericValues[index]);
            if (coercion == String.class || coercion == Boolean.class || coercion == Object.class)
                return (numericValues[index]);
            if (coercion == Byte.class)
                return Byte.parseByte(numericValues[index]);
            if (coercion == Short.class)
                return Short.parseShort(numericValues[index]);
            if (coercion == Long.class)
                return Long.parseLong(numericValues[index]);
            if (coercion == Double.class)
                return Double.parseDouble(numericValues[index]);
            if (coercion == BigDecimal.class)
                return new BigDecimal(numericValues[index]);
        } catch (java.lang.NumberFormatException e) {}
        return null;
    }

    private Object getxxx(int ordinal, Class<?> coercion,
            SQLServerCallableStatement callableStatement) throws SQLException {

        if (coercion == null || coercion == Object.class) {
            return callableStatement.getObject(ordinal);
        } else if (coercion == String.class) {
            return callableStatement.getString(ordinal);
        } else if (coercion == Boolean.class) {
            return Boolean.valueOf(callableStatement.getBoolean(ordinal));
        } else if (coercion == Byte.class) {
            return Byte.valueOf(callableStatement.getByte(ordinal));
        } else if (coercion == Short.class) {
            return Short.valueOf(callableStatement.getShort(ordinal));
        } else if (coercion == Integer.class) {
            return Integer.valueOf(callableStatement.getInt(ordinal));
        } else if (coercion == Long.class) {
            return Long.valueOf(callableStatement.getLong(ordinal));
        } else if (coercion == Float.class) {
            return Float.valueOf(callableStatement.getFloat(ordinal));
        } else if (coercion == Double.class) {
            return Double.valueOf(callableStatement.getDouble(ordinal));
        } else if (coercion == BigDecimal.class) {
            return callableStatement.getBigDecimal(ordinal);
        } else if (coercion == byte[].class) {
            return callableStatement.getBytes(ordinal);
        } else if (coercion == java.sql.Date.class) {
            return callableStatement.getDate(ordinal);
        } else if (coercion == Time.class) {
            return callableStatement.getTime(ordinal);
        } else if (coercion == Timestamp.class) {
            return callableStatement.getTimestamp(ordinal);
        } else if (coercion == microsoft.sql.DateTimeOffset.class) {
            return callableStatement.getDateTimeOffset(ordinal);
        } else {
            // Otherwise
            fail("Unhandled type: " + coercion);
        }

        return null;
    }

    private void createOutputProcedureBinary() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(outputProcedureBinary) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedureBinary);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedureBinary)
                    + " @p0 binary(20) OUTPUT,@p1 varbinary(50) OUTPUT,@p2 varbinary(max) OUTPUT,"
                    + " @p3 binary(512) OUTPUT,@p4 varbinary(8000) OUTPUT " + " AS"
                    + " SELECT top 1 @p0=RandomizedBinary,@p1=DeterministicVarbinary,@p2=DeterministicVarbinaryMax,"
                    + " @p3=DeterministicBinary512,@p4=DeterministicBinary8000 FROM "
                    + AbstractSQLGenerator.escapeIdentifier(BINARY_TABLE_AE);

            stmt.execute(sql);
        }
    }

    private void testOutputProcedureBinaryInorder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.BINARY, 20, 0);
            callableStatement.registerOutParameter(2, java.sql.Types.VARBINARY, 50, 0);
            callableStatement.registerOutParameter(3, java.sql.Types.LONGVARBINARY);
            callableStatement.registerOutParameter(4, java.sql.Types.BINARY, 512, 0);
            callableStatement.registerOutParameter(5, java.sql.Types.VARBINARY, 8000, 0);
            callableStatement.execute();

            byte[] expected = byteValues.get(0);

            byte[] received1 = callableStatement.getBytes(1);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(received1[i], expected[i], TestResource.getResource("R_outputParamFailed"));
            }

            expected = byteValues.get(1);
            byte[] received2 = callableStatement.getBytes(2);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(received2[i], expected[i], TestResource.getResource("R_outputParamFailed"));
            }

            expected = byteValues.get(2);
            byte[] received3 = callableStatement.getBytes(3);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(received3[i], expected[i], TestResource.getResource("R_outputParamFailed"));
            }

            expected = byteValues.get(3);
            byte[] received4 = callableStatement.getBytes(4);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(received4[i], expected[i], TestResource.getResource("R_outputParamFailed"));
            }

            expected = byteValues.get(4);
            byte[] received5 = callableStatement.getBytes(5);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(received5[i], expected[i], TestResource.getResource("R_outputParamFailed"));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testOutputProcedureBinaryInorderObject(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.BINARY, 20, 0);
            callableStatement.registerOutParameter(2, java.sql.Types.VARBINARY, 50, 0);
            callableStatement.registerOutParameter(3, java.sql.Types.LONGVARBINARY);
            callableStatement.registerOutParameter(4, java.sql.Types.BINARY, 512, 0);
            callableStatement.registerOutParameter(5, java.sql.Types.VARBINARY, 8000, 0);
            callableStatement.execute();

            int index = 1;
            for (int i = 0; i < byteValues.size(); i++) {
                byte[] expected = null;
                if (null != byteValues.get(i))
                    expected = byteValues.get(i);

                byte[] binaryValue = (byte[]) callableStatement.getObject(index);
                try {
                    if (null != byteValues.get(i)) {
                        for (int j = 0; j < expected.length; j++) {
                            assertEquals(
                                    expected[j] == binaryValue[j] && expected[j] == binaryValue[j]
                                            && expected[j] == binaryValue[j],
                                    true, "Decryption failed with getObject(): " + binaryValue + ", " + binaryValue
                                            + ", " + binaryValue + ".\n");
                        }
                    }
                } catch (Exception e) {
                    fail(e.getMessage());
                } finally {
                    index++;
                }
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testOutputProcedureBinaryInorderString(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.BINARY, 20, 0);
            callableStatement.registerOutParameter(2, java.sql.Types.VARBINARY, 50, 0);
            callableStatement.registerOutParameter(3, java.sql.Types.LONGVARBINARY);
            callableStatement.registerOutParameter(4, java.sql.Types.BINARY, 512, 0);
            callableStatement.registerOutParameter(5, java.sql.Types.VARBINARY, 8000, 0);
            callableStatement.execute();

            int index = 1;
            for (int i = 0; i < byteValues.size(); i++) {
                String stringValue1 = ("" + callableStatement.getString(index)).trim();

                StringBuffer expected = new StringBuffer();
                String expectedStr = null;

                if (null != byteValues.get(i)) {
                    for (byte b : byteValues.get(i)) {
                        expected.append(String.format("%02X", b));
                    }
                    expectedStr = "" + expected.toString();
                } else {
                    expectedStr = "null";
                }
                try {
                    assertEquals(stringValue1.startsWith(expectedStr), true, "\nDecryption failed with getString(): "
                            + stringValue1 + ".\nExpected Value: " + expectedStr);
                } catch (Exception e) {
                    fail(e.getMessage());
                } finally {
                    index++;
                }
            }
        }
    }

    protected static void createDateTableCallableStatement(String cekName) throws SQLException {
        String sql = "create table " + AbstractSQLGenerator.escapeIdentifier(DATE_TABLE_AE) + " ("
                + "PlainDate date null,"
                + "RandomizedDate date ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicDate date ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainDatetime2Default datetime2 null,"
                + "RandomizedDatetime2Default datetime2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicDatetime2Default datetime2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainDatetimeoffsetDefault datetimeoffset null,"
                + "RandomizedDatetimeoffsetDefault datetimeoffset ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicDatetimeoffsetDefault datetimeoffset ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainTimeDefault time null,"
                + "RandomizedTimeDefault time ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicTimeDefault time ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainDatetime2 datetime2(2) null,"
                + "RandomizedDatetime2 datetime2(2) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicDatetime2 datetime2(2) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainTime time(2) null,"
                + "RandomizedTime time(2) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicTime time(2) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainDatetimeoffset datetimeoffset(2) null,"
                + "RandomizedDatetimeoffset datetimeoffset(2) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicDatetimeoffset datetimeoffset(2) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainDateTime DateTime null,"
                + "RandomizedDateTime DateTime ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicDateTime DateTime ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainSmallDatetime smalldatetime null,"
                + "RandomizedSmallDatetime smalldatetime ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicSmallDatetime smalldatetime ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + ");";

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);
            stmt.execute("DBCC FREEPROCCACHE");
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private static LinkedList<Object> createTemporalTypesCallableStatement(boolean nullable) {

        Date date = RandomData.generateDate(nullable);
        Timestamp datetime2 = RandomData.generateDatetime2(7, nullable);
        DateTimeOffset datetimeoffset = RandomData.generateDatetimeoffset(7, nullable);
        Time time = RandomData.generateTime(7, nullable);
        Timestamp datetime2_2 = RandomData.generateDatetime2(2, nullable);
        Time time_2 = RandomData.generateTime(2, nullable);
        DateTimeOffset datetimeoffset_2 = RandomData.generateDatetimeoffset(2, nullable);
        Timestamp datetime = RandomData.generateDatetime(nullable);
        Timestamp smalldatetime = RandomData.generateSmalldatetime(nullable);

        LinkedList<Object> list = new LinkedList<>();
        list.add(date);
        list.add(datetime2);
        list.add(datetimeoffset);
        list.add(time);
        list.add(datetime2_2);
        list.add(time_2);
        list.add(datetimeoffset_2);
        list.add(datetime);
        list.add(smalldatetime);

        return list;
    }

    private static void populateDateNormalCase() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(DATE_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement sqlPstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // date
            for (int i = 1; i <= 3; i++) {
                sqlPstmt.setDate(i, (Date) dateValues.get(0));
            }

            // datetime2 default
            for (int i = 4; i <= 6; i++) {
                sqlPstmt.setTimestamp(i, (Timestamp) dateValues.get(1));
            }

            // datetimeoffset default
            for (int i = 7; i <= 9; i++) {
                sqlPstmt.setDateTimeOffset(i, (DateTimeOffset) dateValues.get(2));
            }

            // time default
            for (int i = 10; i <= 12; i++) {
                sqlPstmt.setTime(i, (Time) dateValues.get(3));
            }

            // datetime2(2)
            for (int i = 13; i <= 15; i++) {
                sqlPstmt.setTimestamp(i, (Timestamp) dateValues.get(4), 2);
            }

            // time(2)
            for (int i = 16; i <= 18; i++) {
                sqlPstmt.setTime(i, (Time) dateValues.get(5), 2);
            }

            // datetimeoffset(2)
            for (int i = 19; i <= 21; i++) {
                sqlPstmt.setDateTimeOffset(i, (DateTimeOffset) dateValues.get(6), 2);
            }

            // datetime()
            for (int i = 22; i <= 24; i++) {
                sqlPstmt.setDateTime(i, (Timestamp) dateValues.get(7));
            }

            // smalldatetime()
            for (int i = 25; i <= 27; i++) {
                sqlPstmt.setSmallDateTime(i, (Timestamp) dateValues.get(8));
            }
            sqlPstmt.execute();
        }
    }

    private void createOutputProcedureDate() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(outputProcedureDate) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedureDate);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedureDate)
                    + " @p0 date OUTPUT, @p01 date OUTPUT, @p1 datetime2 OUTPUT, @p11 datetime2 OUTPUT,"
                    + " @p2 datetimeoffset OUTPUT, @p21 datetimeoffset OUTPUT, @p3 time OUTPUT, @p31 time OUTPUT, @p4 datetime OUTPUT, @p41 datetime OUTPUT,"
                    + " @p5 smalldatetime OUTPUT, @p51 smalldatetime OUTPUT, @p6 datetime2(2) OUTPUT, @p61 datetime2(2) OUTPUT, @p7 time(2) OUTPUT, @p71 time(2) OUTPUT, "
                    + " @p8 datetimeoffset(2) OUTPUT, @p81 datetimeoffset(2) OUTPUT " + " AS"
                    + " SELECT top 1 @p0=PlainDate,@p01=RandomizedDate,@p1=PlainDatetime2Default,@p11=RandomizedDatetime2Default,"
                    + " @p2=PlainDatetimeoffsetDefault,@p21=DeterministicDatetimeoffsetDefault,"
                    + " @p3=PlainTimeDefault,@p31=DeterministicTimeDefault,"
                    + " @p4=PlainDateTime,@p41=DeterministicDateTime, @p5=PlainSmallDateTime,@p51=RandomizedSmallDateTime, "
                    + " @p6=PlainDatetime2,@p61=RandomizedDatetime2, @p7=PlainTime,@p71=Deterministictime, "
                    + " @p8=PlainDatetimeoffset, @p81=RandomizedDatetimeoffset" + " FROM "
                    + AbstractSQLGenerator.escapeIdentifier(DATE_TABLE_AE);

            stmt.execute(sql);
        }
    }

    private void testOutputProcedureDateInorder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.DATE);
            callableStatement.registerOutParameter(2, java.sql.Types.DATE);
            callableStatement.registerOutParameter(3, java.sql.Types.TIMESTAMP);
            callableStatement.registerOutParameter(4, java.sql.Types.TIMESTAMP);
            callableStatement.registerOutParameter(5, microsoft.sql.Types.DATETIMEOFFSET);
            callableStatement.registerOutParameter(6, microsoft.sql.Types.DATETIMEOFFSET);
            callableStatement.registerOutParameter(7, java.sql.Types.TIME);
            callableStatement.registerOutParameter(8, java.sql.Types.TIME);
            callableStatement.registerOutParameter(9, microsoft.sql.Types.DATETIME); // datetime
            callableStatement.registerOutParameter(10, microsoft.sql.Types.DATETIME); // datetime
            callableStatement.registerOutParameter(11, microsoft.sql.Types.SMALLDATETIME); // smalldatetime
            callableStatement.registerOutParameter(12, microsoft.sql.Types.SMALLDATETIME); // smalldatetime
            callableStatement.registerOutParameter(13, java.sql.Types.TIMESTAMP, 2);
            callableStatement.registerOutParameter(14, java.sql.Types.TIMESTAMP, 2);
            callableStatement.registerOutParameter(15, java.sql.Types.TIME, 2);
            callableStatement.registerOutParameter(16, java.sql.Types.TIME, 2);
            callableStatement.registerOutParameter(17, microsoft.sql.Types.DATETIMEOFFSET, 2);
            callableStatement.registerOutParameter(18, microsoft.sql.Types.DATETIMEOFFSET, 2);
            callableStatement.execute();

            assertEquals(callableStatement.getDate(1), callableStatement.getDate(2),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getTimestamp(3), callableStatement.getTimestamp(4),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getDateTimeOffset(5), callableStatement.getDateTimeOffset(6),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getTime(7), callableStatement.getTime(8),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getDateTime(9), // actual plain
                    callableStatement.getDateTime(10), // received expected enc
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getSmallDateTime(11), callableStatement.getSmallDateTime(12),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getTimestamp(13), callableStatement.getTimestamp(14),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getTime(15).getTime(), callableStatement.getTime(16).getTime(),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getDateTimeOffset(17), callableStatement.getDateTimeOffset(18),
                    TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testOutputProcedureDateInorderObject(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.DATE);
            callableStatement.registerOutParameter(2, java.sql.Types.DATE);
            callableStatement.registerOutParameter(3, java.sql.Types.TIMESTAMP);
            callableStatement.registerOutParameter(4, java.sql.Types.TIMESTAMP);
            callableStatement.registerOutParameter(5, microsoft.sql.Types.DATETIMEOFFSET);
            callableStatement.registerOutParameter(6, microsoft.sql.Types.DATETIMEOFFSET);
            callableStatement.registerOutParameter(7, java.sql.Types.TIME);
            callableStatement.registerOutParameter(8, java.sql.Types.TIME);
            callableStatement.registerOutParameter(9, microsoft.sql.Types.DATETIME); // datetime
            callableStatement.registerOutParameter(10, microsoft.sql.Types.DATETIME); // datetime
            callableStatement.registerOutParameter(11, microsoft.sql.Types.SMALLDATETIME); // smalldatetime
            callableStatement.registerOutParameter(12, microsoft.sql.Types.SMALLDATETIME); // smalldatetime
            callableStatement.registerOutParameter(13, java.sql.Types.TIMESTAMP, 2);
            callableStatement.registerOutParameter(14, java.sql.Types.TIMESTAMP, 2);
            callableStatement.registerOutParameter(15, java.sql.Types.TIME, 2);
            callableStatement.registerOutParameter(16, java.sql.Types.TIME, 2);
            callableStatement.registerOutParameter(17, microsoft.sql.Types.DATETIMEOFFSET, 2);
            callableStatement.registerOutParameter(18, microsoft.sql.Types.DATETIMEOFFSET, 2);
            callableStatement.execute();

            assertEquals(callableStatement.getObject(1), callableStatement.getObject(2),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getObject(3), callableStatement.getObject(4),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getObject(5), callableStatement.getObject(6),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getObject(7), callableStatement.getObject(8),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getObject(9), // actual plain
                    callableStatement.getObject(10), // received expected enc
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getObject(11), callableStatement.getObject(12),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getObject(13), callableStatement.getObject(14),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getObject(15), callableStatement.getObject(16),
                    TestResource.getResource("R_outputParamFailed"));
            assertEquals(callableStatement.getObject(17), callableStatement.getObject(18),
                    TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createOutputProcedureBatch() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(outputProcedureBatch) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedureBatch);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            // If a procedure contains more than one SQL statement, it is considered
            // to be a batch of SQL statements.
            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedureBatch)
                    + " @p0 int OUTPUT, @p1 float OUTPUT, @p2 smallint OUTPUT, @p3 smallmoney OUTPUT " + " AS"
                    + " select top 1 @p0=RandomizedInt FROM " + AbstractSQLGenerator.escapeIdentifier(table3)
                    + " select top 1 @p1=DeterministicFloatDefault FROM "
                    + AbstractSQLGenerator.escapeIdentifier(table3) + " select top 1 @p2=RandomizedSmallint FROM "
                    + AbstractSQLGenerator.escapeIdentifier(table3) + " select top 1 @p3=DeterministicSmallMoney FROM "
                    + AbstractSQLGenerator.escapeIdentifier(table3);

            stmt.execute(sql);
        }
    }

    private void testOutputProcedureBatchInorder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.INTEGER);
            callableStatement.registerOutParameter(2, java.sql.Types.DOUBLE);
            callableStatement.registerOutParameter(3, java.sql.Types.SMALLINT);
            callableStatement.registerOutParameter(4, microsoft.sql.Types.SMALLMONEY);
            callableStatement.execute();

            int intValue2 = callableStatement.getInt(1);
            assertEquals("" + intValue2, numericValues[3], TestResource.getResource("R_outputParamFailed"));

            double floatValue0 = callableStatement.getDouble(2);
            assertEquals("" + floatValue0, numericValues[5], TestResource.getResource("R_outputParamFailed"));

            short shortValue3 = callableStatement.getShort(3);
            assertEquals("" + shortValue3, numericValues[2], TestResource.getResource("R_outputParamFailed"));

            BigDecimal smallmoneyValue = callableStatement.getSmallMoney(4);
            assertEquals("" + smallmoneyValue, numericValues[12], TestResource.getResource("R_outputParamFailed"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createOutputProcedure4() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(outputProcedure4) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedure4);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "create procedure " + AbstractSQLGenerator.escapeIdentifier(outputProcedure4)
                    + " @in1 int, @in2 smallint, @in3 bigint, @in4 int, @in5 smallint, @in6 bigint, @out1 int output, @out2 smallint output, @out3 bigint output, @out4 int output, @out5 smallint output, @out6 bigint output"
                    + " as " + " insert into " + AbstractSQLGenerator.escapeIdentifier(table5)
                    + " values (@in1, @in2, @in3)" + " insert into " + AbstractSQLGenerator.escapeIdentifier(table6)
                    + " values (@in4, @in5, @in6)" + " select * from " + AbstractSQLGenerator.escapeIdentifier(table5)
                    + " select * from " + AbstractSQLGenerator.escapeIdentifier(table6)
                    + " select @out1 = c1, @out2=c2, @out3=c3 from " + AbstractSQLGenerator.escapeIdentifier(table5)
                    + " select @out4 = c1, @out5=c2, @out6=c3 from " + AbstractSQLGenerator.escapeIdentifier(table6);

            stmt.execute(sql);
        }
    }

    private void createMixedProcedureDateScale() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(outputProcedureDateScale)
                + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)" + " DROP PROCEDURE "
                + AbstractSQLGenerator.escapeIdentifier(outputProcedureDateScale);

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            stmt.execute(sql);

            sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedureDateScale)
                    + " @p1 datetime2(2) OUTPUT, @p2 datetime2(2) OUTPUT,"
                    + " @p3 time(2) OUTPUT, @p4 time(2) OUTPUT, @p5 datetimeoffset(2) OUTPUT, @p6 datetimeoffset(2) OUTPUT "
                    + " AS"
                    + " SELECT top 1 @p1=DeterministicDatetime2,@p2=RandomizedDatetime2,@p3=DeterministicTime,@p4=RandomizedTime,"
                    + " @p5=DeterministicDatetimeoffset,@p6=RandomizedDatetimeoffset " + " FROM "
                    + AbstractSQLGenerator.escapeIdentifier(SCALE_DATE_TABLE_AE)
                    + " where DeterministicDatetime2 = @p1 and DeterministicTime = @p3 and DeterministicDatetimeoffset=@p5";

            stmt.execute(sql);
        }
    }

    private void testMixedProcedureDateScaleInorder(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter(1, java.sql.Types.TIMESTAMP, 2);
            callableStatement.registerOutParameter(2, java.sql.Types.TIMESTAMP, 2);
            callableStatement.registerOutParameter(3, java.sql.Types.TIME, 2);
            callableStatement.registerOutParameter(4, java.sql.Types.TIME, 2);
            callableStatement.registerOutParameter(5, microsoft.sql.Types.DATETIMEOFFSET, 2);
            callableStatement.registerOutParameter(6, microsoft.sql.Types.DATETIMEOFFSET, 2);
            callableStatement.setTimestamp(1, (Timestamp) dateValues.get(4), 2);
            callableStatement.setTime(3, (Time) dateValues.get(5), 2);
            callableStatement.setDateTimeOffset(5, (DateTimeOffset) dateValues.get(6), 2);
            callableStatement.execute();

            assertEquals(callableStatement.getTimestamp(1), callableStatement.getTimestamp(2),
                    TestResource.getResource("R_outputParamFailed"));

            assertEquals(callableStatement.getTime(3), callableStatement.getTime(4),
                    TestResource.getResource("R_outputParamFailed"));

            assertEquals(callableStatement.getDateTimeOffset(5), callableStatement.getDateTimeOffset(6),
                    TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testMixedProcedureDateScaleWithParameterName(String sql) throws SQLException {

        try (Connection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) TestUtils
                        .getCallableStmt(con, sql, stmtColEncSetting)) {

            callableStatement.registerOutParameter("p1", java.sql.Types.TIMESTAMP, 2);
            callableStatement.registerOutParameter("p2", java.sql.Types.TIMESTAMP, 2);
            callableStatement.registerOutParameter("p3", java.sql.Types.TIME, 2);
            callableStatement.registerOutParameter("p4", java.sql.Types.TIME, 2);
            callableStatement.registerOutParameter("p5", microsoft.sql.Types.DATETIMEOFFSET, 2);
            callableStatement.registerOutParameter("p6", microsoft.sql.Types.DATETIMEOFFSET, 2);
            callableStatement.setTimestamp("p1", (Timestamp) dateValues.get(4), 2);
            callableStatement.setTime("p3", (Time) dateValues.get(5), 2);
            callableStatement.setDateTimeOffset("p5", (DateTimeOffset) dateValues.get(6), 2);
            callableStatement.execute();

            assertEquals(callableStatement.getTimestamp(1), callableStatement.getTimestamp(2),
                    TestResource.getResource("R_outputParamFailed"));

            assertEquals(callableStatement.getTime(3), callableStatement.getTime(4),
                    TestResource.getResource("R_outputParamFailed"));

            assertEquals(callableStatement.getDateTimeOffset(5), callableStatement.getDateTimeOffset(6),
                    TestResource.getResource("R_outputParamFailed"));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
