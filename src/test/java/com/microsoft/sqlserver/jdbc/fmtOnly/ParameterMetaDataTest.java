package com.microsoft.sqlserver.jdbc.fmtOnly;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


public class ParameterMetaDataTest extends AbstractTest {

    private static final String tableName = "[test_jdbc_" + UUID.randomUUID() + "]";

    @BeforeEach
    public void setupTests() throws SQLException {
        try (Connection c = getConnection(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE " + tableName
                    + "(cBigint bigint, cNumeric numeric, cBit bit, cSmallint smallint, cDecimal decimal, "
                    + "cSmallmoney smallmoney, cInt int, cTinyint tinyint, cMoney money, cFloat float, cReal real, "
                    + "cDate date, cDatetimeoffset datetimeoffset, cDatetime2 datetime2, cSmalldatetime smalldatetime, "
                    + "cDatetime datetime, cTime time, cChar char, cVarchar varchar(8000), cNchar nchar, "
                    + "cNvarchar nvarchar(4000), cBinary binary, cVarbinary varbinary(8000))");
        }
    }

    @AfterEach
    public void cleanupTests() throws SQLException {
        try (Connection c = getConnection(); Statement s = c.createStatement()) {
            TestUtils.dropTableIfExists(tableName, s);
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void compareStoredProcTest() throws SQLException {
        List<String> l = Arrays.asList("SELECT * FROM " + tableName + " WHERE cBigint > ?",
                "SELECT TOP(20) PERCENT * FROM " + tableName + " WHERE cBigint > ?",
                "SELECT TOP(20) * FROM " + tableName + " WHERE cFloat < ?",
                "SELECT TOP 20 * FROM " + tableName + " WHERE cReal = ?",
                "SELECT TOP(20) PERCENT WITH TIES * FROM " + tableName
                        + " WHERE cInt BETWEEN ? AND ? AND ? = cChar ORDER BY cDecimal ASC",
                "INSERT " + tableName + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                "INSERT " + tableName + "(cInt,cVarchar,cNvarchar) VALUES(?,?,?)",
                "INSERT " + tableName + "(cInt,cVarchar,cNvarchar) VALUES(?,?,?),(?,?,?),(?,?,?)",
                "DELETE " + tableName + " WHERE cFloat >= ? AND ? <= cInt",
                "DELETE FROM " + tableName + " WHERE cReal BETWEEN ? AND ?;",
                "DELETE " + tableName + " WHERE cFloat IN (SELECT cFloat FROM " + tableName + " WHERE cReal = ?)",
                "DELETE " + tableName + " WHERE cFloat IN (SELECT cFloat FROM " + tableName + " t WHERE t.cReal = ?)",
                "UPDATE TOP (10) " + tableName + " SET cInt = cInt + ?;",
                "UPDATE TOP (10) " + tableName + " SET cInt = cInt + ? FROM (SELECT TOP 10 cFloat,cReal,cNvarchar FROM "
                        + tableName + " ORDER BY cFloat ASC) AS b WHERE b.cReal > ? AND b.cNvarchar LIKE ?",
                "WITH t1(cInt,cReal,cVarchar,cMoney) AS (SELECT cInt,cReal,cVarchar,cMoney FROM " + tableName
                        + ") UPDATE a SET a.cInt = ? * a.cInt FROM " + tableName
                        + " AS a JOIN t1 AS b ON a.cInt = b.cInt WHERE b.cInt = ?",
                "SELECT cInt FROM " + tableName + " WHERE ? = (cInt + (3 - 5))",
                "SELECT cInt FROM " + tableName + " WHERE (cInt + (3 - 5)) = ?",
                "SELECT cInt FROM " + tableName + " WHERE " + tableName + ".[cInt] = ?",
                "SELECT cInt FROM " + tableName + " WHERE ? = " + tableName + ".[cInt]",
                "WITH t1(cInt) AS (SELECT 1), t2(cInt) AS (SELECT 2) SELECT * FROM t1 JOIN t2 ON [t1].\"cInt\" = \"t2\".[cInt] WHERE \"t1\".[cInt] = [t2].\"cInt\" + ?",
                "INSERT INTO " + tableName + "(cInt,cFloat) SELECT 1,1.5 WHERE 1 > ?",
                "WITH t1(cInt) AS (SELECT 1), t2(cInt) AS (SELECT 2), t3(cInt) AS (SELECT 3) SELECT * FROM t1,t2,t3 WHERE t1.cInt >= ?",
                "SELECT (1),2,[cInt],\"cFloat\" FROM " + tableName + " WHERE cNvarchar LIKE ?",
                "WITH t1(cInt) AS (SELECT 1) SELECT * FROM \"t1\"");
        l.forEach(this::compareFmtAndSp);
    }

    @Test
    public void noStoredProcTest() throws SQLException {
        List<String> l = Arrays.asList("SELECT cMoney FROM " + tableName + " WHERE cMoney > $?",
                "SELECT cVarchar FROM " + tableName + " WHERE " + tableName + ".[cVarchar] NOT LIKE ?",
                "SELECT cVarchar FROM " + tableName + " WHERE ? NOT LIKE " + tableName + ".[cVarchar]",
                "INSERT " + tableName + "(cInt) VALUES((1+?)),(3*(?+3))", "SELECT ?");
        l.forEach(this::executeFmt);
    }

    @Test
    public void exceptionTest() throws SQLException {
        executeInvalidFmt("SELECT FROM OPENQUERY INVALID TSQL", "R_invalidOpenqueryCall");
        executeInvalidFmt("INSERT INTO OPENXML INVALID TSQL VALUES (?,?,?)", "R_invalidOpenqueryCall");
        executeInvalidFmt("WITH INVALID_CTE AS SELECT * FROM FOO", "R_invalidCTEFormat");
    }

    @Test
    public void tempTableTest() throws SQLException {
        String tempTableName = "[#jdbc_temp" + UUID.randomUUID() + "]";
        try (Connection c = PrepUtil.getConnection(AbstractTest.connectionString + ";useFmtOnly=true;");
                Statement s = c.createStatement()) {
            TestUtils.dropTableIfExists(tempTableName, s);
            s.execute("CREATE TABLE " + tempTableName + " (c1 int)");
            try (PreparedStatement p = c.prepareStatement("SELECT * FROM " + tempTableName + " WHERE c1 = ?")) {
                ParameterMetaData pmd = p.getParameterMetaData();
                assertTrue(pmd.getParameterCount() == 1);
            }
        } finally {
            try (Statement s = connection.createStatement()) {
                TestUtils.dropTableIfExists(tempTableName, s);
            }
        }
    }

    @Test
    public void viewTest() throws SQLException {
        String tempViewName = "[jdbc_view" + UUID.randomUUID() + "]";
        try (Connection c = PrepUtil.getConnection(AbstractTest.connectionString + ";useFmtOnly=true;");
                Statement s = c.createStatement()) {
            TestUtils.dropViewIfExists(tempViewName, s);
            s.execute("CREATE VIEW " + tempViewName + " AS SELECT cBigInt FROM" + tableName);
            try (PreparedStatement p = c.prepareStatement("SELECT * FROM " + tempViewName + " WHERE cBigInt = ?")) {
                ParameterMetaData pmd = p.getParameterMetaData();
                assertTrue(pmd.getParameterCount() == 1);
            }
        } finally {
            try (Statement s = connection.createStatement()) {
                TestUtils.dropViewIfExists(tempViewName, s);
            }
        }
    }

    private void executeFmt(String userSQL) {
        try (Connection c = PrepUtil.getConnection(AbstractTest.connectionString + ";useFmtOnly=true;");
                PreparedStatement pstmt = c.prepareStatement(userSQL)) {
            ParameterMetaData pmd = pstmt.getParameterMetaData();
            for (int i = 1; i <= pmd.getParameterCount(); i++) {
                pmd.getParameterClassName(i);
                pmd.getParameterMode(i);
                pmd.getParameterType(i);
                pmd.getParameterTypeName(i);
                pmd.getPrecision(i);
                pmd.getScale(i);
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    private void executeInvalidFmt(String userSQL, String expectedError) {

        try (Connection c = PrepUtil.getConnection(AbstractTest.connectionString + ";useFmtOnly=true;");
                PreparedStatement pstmt = c.prepareStatement(userSQL)) {
            pstmt.getParameterMetaData();
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg(expectedError)));
        }
    }

    private void compareFmtAndSp(String userSQL) {
        try (Connection c = getConnection();
                Connection c2 = PrepUtil.getConnection(AbstractTest.connectionString + ";useFmtOnly=true;");
                PreparedStatement stmt1 = c.prepareStatement(userSQL);
                PreparedStatement stmt2 = c2.prepareStatement(userSQL)) {
            ParameterMetaData pmd1 = stmt1.getParameterMetaData();
            ParameterMetaData pmd2 = stmt2.getParameterMetaData();
            assertEquals(pmd1.getParameterCount(), pmd2.getParameterCount());
            for (int i = 1; i <= pmd1.getParameterCount(); i++) {
                assertEquals(pmd1.getParameterClassName(i), pmd2.getParameterClassName(i));
                assertEquals(pmd1.getParameterMode(i), pmd2.getParameterMode(i));
                assertEquals(pmd1.getParameterType(i), pmd2.getParameterType(i));
                assertEquals(pmd1.getParameterTypeName(i), pmd2.getParameterTypeName(i));
                assertEquals(pmd1.getPrecision(i), pmd2.getPrecision(i));
                assertEquals(pmd1.getScale(i), pmd2.getScale(i));
            }
        } catch (SQLException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }
}
